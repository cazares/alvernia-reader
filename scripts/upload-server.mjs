import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { spawnSync } from "node:child_process";
import Busboy from "busboy";

const PORT = Number(process.env.UPLOAD_SERVER_PORT || 8787);
const DATA_ROOT = process.env.NUESTRO_CORO_DATA
  || path.join(os.homedir(), "Library", "Application Support", "NuestroCoro");
const UPLOADS_DIR = path.join(DATA_ROOT, "uploads");
const ACTIVE_DIR = path.join(DATA_ROOT, "active");
const ACTIVE_PAGES_DIR = path.join(ACTIVE_DIR, "pages");
const LATEST_PATH = path.join(DATA_ROOT, "latest.json");

const ALLOWED_EXTENSIONS = new Set(["pdf", "key"]);

const ensureDirs = () => {
  fs.mkdirSync(UPLOADS_DIR, { recursive: true });
  fs.mkdirSync(ACTIVE_PAGES_DIR, { recursive: true });
};

const sanitizeFilename = (name = "archivo") => name.replace(/[^a-zA-Z0-9._-]/g, "_");

const extractExtension = (name = "") => {
  const parts = name.toLowerCase().split(".");
  return parts.length > 1 ? parts.pop() : "";
};

const readLatest = () => {
  if (!fs.existsSync(LATEST_PATH)) return null;
  const raw = fs.readFileSync(LATEST_PATH, "utf8");
  return JSON.parse(raw);
};

const writeLatest = (payload) => {
  fs.writeFileSync(LATEST_PATH, JSON.stringify(payload, null, 2));
};

const htmlResponse = (res, body, status = 200) => {
  res.writeHead(status, {
    "content-type": "text/html; charset=utf-8",
    "cache-control": "no-store",
  });
  res.end(body);
};

const jsonResponse = (res, body, status = 200) => {
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store",
  });
  res.end(JSON.stringify(body));
};

const sendFile = (res, filePath, contentType) => {
  if (!fs.existsSync(filePath)) {
    res.writeHead(404);
    res.end("No encontrado");
    return;
  }
  res.writeHead(200, {
    "content-type": contentType,
    "cache-control": "no-store",
  });
  fs.createReadStream(filePath).pipe(res);
};

const uploadPage = `<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Subir archivo</title>
    <style>
      body { font-family: system-ui, -apple-system, "Segoe UI", sans-serif; background: #0b0b0b; color: #f7f7f7; margin: 0; display: grid; place-items: center; min-height: 100vh; }
      .card { width: min(520px, 92vw); background: #14171d; border: 1px solid #2b2f38; border-radius: 20px; padding: 24px; box-shadow: 0 20px 50px rgba(0,0,0,0.35); }
      h1 { margin: 0 0 8px; font-size: 1.4rem; }
      p { margin: 0 0 18px; color: #b9c0cc; }
      input[type=file] { width: 100%; margin-bottom: 16px; }
      button { width: 100%; padding: 12px 16px; border-radius: 14px; border: 0; background: #2e81e8; color: #fff; font-weight: 600; font-size: 1rem; cursor: pointer; }
      .hint { font-size: 0.85rem; margin-top: 12px; color: #8b93a1; }
    </style>
  </head>
  <body>
    <form class="card" method="post" enctype="multipart/form-data">
      <h1>Subir archivo</h1>
      <p>Archivos soportados: Keynote (.key) o PDF.</p>
      <input type="file" name="file" accept=".key,.pdf,application/pdf" required />
      <button type="submit">Subir</button>
      <p class="hint">Luego entra a <strong>/promote</strong> para activar el nuevo archivo.</p>
    </form>
  </body>
</html>`;

const promotePage = (latest) => {
  if (!latest) {
    return "<p style=\"font-family:system-ui;\">No hay archivos subidos todavía.</p>";
  }

  const status = latest.promotedAt
    ? `Activo desde ${latest.promotedAt}`
    : latest.promoteRequestedAt
      ? `Promote solicitado ${latest.promoteRequestedAt}`
      : "Listo para promocionar";

  return `<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Promote</title>
    <style>
      body { font-family: system-ui, -apple-system, "Segoe UI", sans-serif; background: #0b0b0b; color: #f7f7f7; margin: 0; display: grid; place-items: center; min-height: 100vh; }
      .card { width: min(520px, 92vw); background: #14171d; border: 1px solid #2b2f38; border-radius: 20px; padding: 24px; box-shadow: 0 20px 50px rgba(0,0,0,0.35); }
      button { width: 100%; padding: 12px 16px; border-radius: 14px; border: 0; background: #2e81e8; color: #fff; font-weight: 600; font-size: 1rem; cursor: pointer; }
      .meta { font-size: 0.9rem; color: #9aa3b2; margin-bottom: 18px; }
    </style>
  </head>
  <body>
    <form class="card" method="post">
      <h1>Promocionar archivo</h1>
      <p>Archivo: <strong>${latest.uploadName ?? "(sin nombre)"}</strong></p>
      <div class="meta">Estado: ${status}</div>
      <button type="submit">Promote ahora</button>
    </form>
  </body>
</html>`;
};

const ensureBinary = (name, hint) => {
  const result = spawnSync("/usr/bin/which", [name]);
  if (result.status !== 0) {
    throw new Error(`${name} no encontrado. ${hint}`);
  }
};

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

const renderPdfPages = (pdfPath) => {
  fs.rmSync(ACTIVE_PAGES_DIR, { recursive: true, force: true });
  fs.mkdirSync(ACTIVE_PAGES_DIR, { recursive: true });
  const outputPrefix = path.join(ACTIVE_PAGES_DIR, "page");
  const result = spawnSync(
    "pdftoppm",
    ["-jpeg", "-jpegopt", "quality=80", "-r", "144", pdfPath, outputPrefix],
    { stdio: "inherit" },
  );

  if (result.status !== 0) {
    throw new Error(`pdftoppm fallo con codigo ${result.status ?? 1}`);
  }

  const rawFiles = fs
    .readdirSync(ACTIVE_PAGES_DIR)
    .filter((file) => /^page-\d+\.jpg$/.test(file))
    .sort((a, b) => a.localeCompare(b));

  for (const file of rawFiles) {
    const match = file.match(/page-(\d+)\.jpg/);
    if (!match) continue;
    const index = Number(match[1]);
    const padded = String(index).padStart(3, "0");
    const newName = `page-${padded}.jpg`;
    fs.renameSync(path.join(ACTIVE_PAGES_DIR, file), path.join(ACTIVE_PAGES_DIR, newName));
  }

  const pageFiles = fs
    .readdirSync(ACTIVE_PAGES_DIR)
    .filter((file) => /^page-\d+\.jpg$/.test(file))
    .sort((a, b) => a.localeCompare(b));

  const songIndex = pageFiles.map((file, idx) => ({
    song: idx + 1,
    page: idx + 1,
  }));

  fs.writeFileSync(
    path.join(ACTIVE_DIR, "pages.json"),
    JSON.stringify({ totalPages: pageFiles.length, songIndex }),
  );
};

const handleUpload = (req, res) => {
  if (req.method === "GET") {
    htmlResponse(res, uploadPage);
    return;
  }

  if (req.method !== "POST") {
    res.writeHead(405);
    res.end("Metodo no permitido");
    return;
  }

  const bb = Busboy({ headers: req.headers });
  let uploadInfo = null;
  let fileRejected = null;

  bb.on("file", (_, file, info) => {
    const filename = info?.filename || "archivo";
    const extension = extractExtension(filename);
    if (!ALLOWED_EXTENSIONS.has(extension)) {
      fileRejected = "Solo .key o .pdf.";
      file.resume();
      return;
    }

    const safeName = sanitizeFilename(filename);
    const outputPath = path.join(UPLOADS_DIR, `${Date.now()}-${safeName}`);
    const outStream = fs.createWriteStream(outputPath);
    file.pipe(outStream);

    uploadInfo = {
      uploadName: filename,
      uploadPath: outputPath,
      extension,
    };
  });

  bb.on("close", () => {
    if (fileRejected) {
      htmlResponse(res, `<p style=\"font-family:system-ui;\">${fileRejected}</p>`, 400);
      return;
    }

    if (!uploadInfo) {
      htmlResponse(res, "<p style=\"font-family:system-ui;\">Archivo invalido.</p>", 400);
      return;
    }

    const now = new Date().toISOString();
    const latest = {
      uploadName: uploadInfo.uploadName,
      uploadPath: uploadInfo.uploadPath,
      uploadedAt: now,
      status: uploadInfo.extension === "pdf" ? "ready" : "uploaded",
      pdfPath: uploadInfo.extension === "pdf" ? uploadInfo.uploadPath : null,
      promoteRequestedAt: null,
      promotedAt: null,
    };

    writeLatest(latest);

    htmlResponse(res, `<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Subido</title>
    <style>
      body { font-family: system-ui, -apple-system, "Segoe UI", sans-serif; background: #0b0b0b; color: #f7f7f7; margin: 0; display: grid; place-items: center; min-height: 100vh; }
      .card { width: min(520px, 92vw); background: #14171d; border: 1px solid #2b2f38; border-radius: 20px; padding: 24px; box-shadow: 0 20px 50px rgba(0,0,0,0.35); text-align: center; }
      a { display: inline-block; margin-top: 12px; padding: 10px 16px; border-radius: 14px; background: #2e81e8; color: #fff; font-weight: 600; text-decoration: none; }
    </style>
  </head>
  <body>
    <div class="card">
      <div>Archivo subido.</div>
      <a href="/promote">Ir a Promote</a>
    </div>
  </body>
</html>`);
  });

  req.pipe(bb);
};

const handlePromote = (req, res) => {
  if (req.method === "GET") {
    htmlResponse(res, promotePage(readLatest()));
    return;
  }

  if (req.method !== "POST") {
    res.writeHead(405);
    res.end("Metodo no permitido");
    return;
  }

  const latest = readLatest();
  if (!latest) {
    htmlResponse(res, "<p style=\"font-family:system-ui;\">No hay archivos para promover.</p>", 404);
    return;
  }

  try {
    let pdfPath = latest.pdfPath;
    if (!pdfPath) {
      const outputPdfPath = path.join(UPLOADS_DIR, `converted-${Date.now()}.pdf`);
      exportKeynoteToPdf(latest.uploadPath, outputPdfPath);
      pdfPath = outputPdfPath;
      latest.pdfPath = outputPdfPath;
      latest.status = "ready";
      latest.convertedAt = new Date().toISOString();
    }

    renderPdfPages(pdfPath);
    latest.promotedAt = new Date().toISOString();
    latest.promoteRequestedAt = latest.promoteRequestedAt ?? latest.promotedAt;
    latest.status = "promoted";
    writeLatest(latest);

    htmlResponse(res, "<p style=\"font-family:system-ui;\">Promote completado.</p>");
  } catch (error) {
    htmlResponse(res, `<p style=\"font-family:system-ui;\">Error: ${error.message}</p>`, 500);
  }
};

const handleDownload = (req, res) => {
  const latest = readLatest();
  if (!latest?.pdfPath) {
    htmlResponse(res, "<p style=\"font-family:system-ui;\">No hay PDF listo.</p>", 404);
    return;
  }

  const name = sanitizeFilename(latest.uploadName || "archivo.pdf");
  res.writeHead(200, {
    "content-type": "application/pdf",
    "content-disposition": `attachment; filename=\"${name}\"`,
    "cache-control": "no-store",
  });
  fs.createReadStream(latest.pdfPath).pipe(res);
};

const handlePagesJson = (req, res) => {
  const jsonPath = path.join(ACTIVE_DIR, "pages.json");
  if (!fs.existsSync(jsonPath)) {
    res.writeHead(404);
    res.end("No encontrado");
    return;
  }
  sendFile(res, jsonPath, "application/json");
};

const handlePageImage = (req, res) => {
  const fileName = req.url.replace("/pages/", "");
  const filePath = path.join(ACTIVE_PAGES_DIR, fileName);
  sendFile(res, filePath, "image/jpeg");
};

const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (url.pathname === "/upload") {
    handleUpload(req, res);
    return;
  }

  if (url.pathname === "/promote") {
    handlePromote(req, res);
    return;
  }

  if (url.pathname === "/download") {
    handleDownload(req, res);
    return;
  }

  if (url.pathname === "/pages.json") {
    handlePagesJson(req, res);
    return;
  }

  if (url.pathname.startsWith("/pages/")) {
    handlePageImage(req, res);
    return;
  }

  jsonResponse(res, { status: "ok" });
});

ensureDirs();
ensureBinary("pdftoppm", "Instala con: brew install poppler");
ensureBinary("osascript", "Requiere macOS con Keynote instalado.");

server.listen(PORT, () => {
  console.log(`Upload server listo en http://localhost:${PORT}`);
});
