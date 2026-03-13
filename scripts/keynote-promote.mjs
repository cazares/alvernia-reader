import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { spawnSync } from "node:child_process";
import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";

const REQUIRED_ENV = [
  "R2_BUCKET",
  "R2_ENDPOINT",
  "R2_ACCESS_KEY_ID",
  "R2_SECRET_ACCESS_KEY",
];

const ACTIVE_PREFIX = process.env.R2_ACTIVE_PREFIX ?? "active";
const UPLOAD_PREFIX = process.env.R2_UPLOAD_PREFIX ?? "uploads/";
const LATEST_KEY = process.env.R2_LATEST_KEY ?? "latest.json";
const POLL_MS = Number(process.env.POLL_MS ?? 10000);
const RUN_ONCE = process.argv.includes("--once");

const ensureEnv = () => {
  const missing = REQUIRED_ENV.filter((key) => !process.env[key]);
  if (missing.length > 0) {
    throw new Error(`Faltan variables de entorno: ${missing.join(", ")}`);
  }
};

const ensureBinary = (name, hint) => {
  const result = spawnSync("/usr/bin/which", [name]);
  if (result.status !== 0) {
    throw new Error(`${name} no encontrado. ${hint}`);
  }
};

const s3 = new S3Client({
  region: "auto",
  endpoint: process.env.R2_ENDPOINT,
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY_ID,
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
  },
});

const streamToBuffer = async (stream) => {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
};

const readJson = async (key) => {
  try {
    const response = await s3.send(new GetObjectCommand({
      Bucket: process.env.R2_BUCKET,
      Key: key,
    }));
    const body = await streamToBuffer(response.Body);
    return JSON.parse(body.toString("utf8"));
  } catch (error) {
    if (error?.$metadata?.httpStatusCode === 404) return null;
    throw error;
  }
};

const writeJson = async (key, value) => {
  await s3.send(new PutObjectCommand({
    Bucket: process.env.R2_BUCKET,
    Key: key,
    Body: JSON.stringify(value, null, 2),
    ContentType: "application/json",
  }));
};

const downloadToFile = async (key, outputPath) => {
  const response = await s3.send(new GetObjectCommand({
    Bucket: process.env.R2_BUCKET,
    Key: key,
  }));
  const body = await streamToBuffer(response.Body);
  await fs.promises.writeFile(outputPath, body);
};

const uploadFile = async (key, filePath, contentType) => {
  await s3.send(new PutObjectCommand({
    Bucket: process.env.R2_BUCKET,
    Key: key,
    Body: fs.createReadStream(filePath),
    ContentType: contentType,
    CacheControl: "no-store",
  }));
};

const normalizeUploadKey = (uploadKey) => uploadKey.replace(/^\/+/, "");

const exportKeynoteToPdf = (inputPath, outputPath) => {
  const escape = (value) => value.replace(/\\/g, "\\\\").replace(/"/g, "\\\"");
  const script = `
set inputFile to POSIX file "${escape(inputPath)}"
set outputFile to POSIX file "${escape(outputPath)}"

tell application "Keynote"
  activate
  set doc to open inputFile
  export doc to outputFile as PDF
  close doc saving no
end tell
`;

  const result = spawnSync("osascript", ["-e", script], { stdio: "inherit" });
  if (result.status !== 0) {
    throw new Error("Keynote fallo al exportar PDF.");
  }
};

const renderPdfPages = (pdfPath, outputDir) => {
  const outputPrefix = path.join(outputDir, "page");
  const result = spawnSync("pdftoppm", [
    "-jpeg",
    "-jpegopt",
    "quality=80",
    "-r",
    "144",
    pdfPath,
    outputPrefix,
  ], { stdio: "inherit" });

  if (result.status !== 0) {
    throw new Error(`pdftoppm fallo con codigo ${result.status ?? 1}`);
  }
};

const buildSongIndex = (totalPages) => {
  const entries = [];
  for (let page = 1; page <= totalPages; page += 1) {
    entries.push({ song: page, page });
  }
  return entries;
};

const promotePdf = async (pdfPath) => {
  const tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), "nuestro-coro-"));
  try {
    renderPdfPages(pdfPath, tempDir);
    const rawFiles = (await fs.promises.readdir(tempDir))
      .filter((file) => /^page-\d+\.jpg$/.test(file))
      .sort((a, b) => a.localeCompare(b));

    if (rawFiles.length === 0) {
      throw new Error("No se generaron paginas JPEG.");
    }

    const renamed = [];
    for (const file of rawFiles) {
      const match = file.match(/page-(\d+)\.jpg/);
      if (!match) continue;
      const index = Number(match[1]);
      const padded = String(index).padStart(3, "0");
      const newName = `page-${padded}.jpg`;
      await fs.promises.rename(path.join(tempDir, file), path.join(tempDir, newName));
      renamed.push(newName);
    }

    const pagesJson = {
      totalPages: renamed.length,
      songIndex: buildSongIndex(renamed.length),
    };

    await writeJson(`${ACTIVE_PREFIX}/pages.json`, pagesJson);

    for (const file of renamed) {
      await uploadFile(`${ACTIVE_PREFIX}/pages/${file}`, path.join(tempDir, file), "image/jpeg");
    }
  } finally {
    await fs.promises.rm(tempDir, { recursive: true, force: true });
  }
};

const ensurePdfForLatest = async (latest) => {
  const uploadKey = normalizeUploadKey(latest.uploadKey);
  if (!uploadKey) {
    throw new Error("latest.json no tiene uploadKey");
  }

  if (latest.pdfKey) {
    return latest;
  }

  if (uploadKey.toLowerCase().endsWith(".pdf")) {
    latest.pdfKey = uploadKey;
    latest.status = "ready";
    await writeJson(LATEST_KEY, latest);
    return latest;
  }

  if (!uploadKey.toLowerCase().endsWith(".key")) {
    throw new Error("El archivo no es PDF ni Keynote.");
  }

  const tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), "nuestro-coro-key-"));
  try {
    const inputKeyPath = path.join(tempDir, path.basename(uploadKey));
    const outputPdfPath = path.join(tempDir, "converted.pdf");

    await downloadToFile(uploadKey, inputKeyPath);
    exportKeynoteToPdf(inputKeyPath, outputPdfPath);

    const pdfKey = `converted/${path.basename(uploadKey, path.extname(uploadKey))}.pdf`;
    await uploadFile(pdfKey, outputPdfPath, "application/pdf");

    latest.pdfKey = pdfKey;
    latest.status = "ready";
    latest.convertedAt = new Date().toISOString();
    await writeJson(LATEST_KEY, latest);
    return latest;
  } finally {
    await fs.promises.rm(tempDir, { recursive: true, force: true });
  }
};

const shouldPromote = (latest) => {
  if (!latest?.promoteRequestedAt) return false;
  if (!latest.promotedAt) return true;
  return new Date(latest.promoteRequestedAt).getTime() > new Date(latest.promotedAt).getTime();
};

const runOnce = async () => {
  const latest = await readJson(LATEST_KEY);
  if (!latest) {
    console.log("No hay latest.json todavía.");
    return;
  }

  if (!shouldPromote(latest)) {
    console.log("No hay promote pendiente.");
    return;
  }

  const updated = await ensurePdfForLatest(latest);
  const tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), "nuestro-coro-pdf-"));
  const pdfPath = path.join(tempDir, "source.pdf");

  try {
    await downloadToFile(updated.pdfKey, pdfPath);
    await promotePdf(pdfPath);
    updated.promotedAt = new Date().toISOString();
    updated.activePrefix = ACTIVE_PREFIX;
    await writeJson(LATEST_KEY, updated);
    console.log("Promote completado.");
  } finally {
    await fs.promises.rm(tempDir, { recursive: true, force: true });
  }
};

const main = async () => {
  ensureEnv();
  ensureBinary("pdftoppm", "Instala con: brew install poppler");
  ensureBinary("osascript", "Requiere macOS con Keynote instalado.");

  if (RUN_ONCE) {
    await runOnce();
    return;
  }

  await runOnce();
  setInterval(runOnce, POLL_MS);
};

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
