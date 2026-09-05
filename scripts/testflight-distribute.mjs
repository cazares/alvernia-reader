#!/usr/bin/env node
// testflight-distribute.mjs — put an uploaded build in front of the choir.
//
// release.sh uploads to TestFlight and stops there: an uploaded build reaches NOBODY until it is
// attached to a beta group. That last step was being done by hand in the App Store Connect UI every
// release, which is both a click-path to forget at 1am and the reason a build can sit "shipped" for
// a day while the choir is still on the previous one.
//
// Auth is the same App Store Connect API key release.sh already uses (scripts/asc-credentials.env +
// ~/.appstoreconnect/private_keys/AuthKey_<id>.p8). The key must carry ADMIN — a lesser role
// authenticates fine and then 403s, which reads like a different failure entirely.
//
//   node scripts/testflight-distribute.mjs                 # newest build, INTERNAL groups only
//   node scripts/testflight-distribute.mjs --build 451
//   node scripts/testflight-distribute.mjs --groups Internal
//   node scripts/testflight-distribute.mjs --list          # show state, change nothing
//   node scripts/testflight-distribute.mjs --build 451 --groups TODOS --allow-external
//                                                          # the ONLY way to reach an external group
//
// EXTERNAL GROUPS ARE THE CHOIR. Miguel, 2026-09-04: never "TODOS", never ANY external testing group —
// he hardware-tests on his own devices first and adds testers himself. Uploading is always fine; it is
// the attachment to an external group that puts a build in front of the congregation. So the default
// here is internal-only, and an external group is reachable only when it is BOTH named with --groups
// AND opted into with --allow-external. release.sh never passes that flag.
// Pinned by e2e/testflightNoExternalGroups.test.mjs, which lifts selectTargets() and executes it.
import fs from "node:fs";
import os from "node:os";
import crypto from "node:crypto";

/**
 * Which beta groups a build may be attached to. Self-contained on purpose — the test lifts this
 * function out of the source and runs it, so it must not close over anything.
 *   - Internal groups: always eligible (subject to --groups if given).
 *   - External groups: eligible ONLY when explicitly named via --groups AND --allow-external is set.
 *     --allow-external alone never means "every external group".
 */
function selectTargets(groups, { only, allowExternal }) {
  const wanted = only ? only.split(",").map((s) => s.trim().toLowerCase()).filter(Boolean) : null;
  return groups.filter((g) => {
    const a = g.attributes;
    if (wanted && !wanted.includes(String(a.name).toLowerCase())) return false;
    if (a.isInternalGroup === true) return true;
    return allowExternal === true && wanted !== null;
  });
}

// Read from app.json rather than hardcoded: a wrong literal here fails as "no app found", which
// reads like an auth or account problem rather than a typo.
const BUNDLE_ID = process.env.ASC_BUNDLE_ID
  || JSON.parse(fs.readFileSync("app.json", "utf8")).expo?.ios?.bundleIdentifier;
const arg = (n, d) => { const i = process.argv.indexOf(`--${n}`); return i > 0 ? process.argv[i + 1] : d; };
const LIST_ONLY = process.argv.includes("--list");

const env = Object.fromEntries(
  fs.readFileSync("scripts/asc-credentials.env", "utf8").split("\n")
    .map((l) => l.trim()).filter((l) => l && !l.startsWith("#"))
    .map((l) => { const i = l.indexOf("="); return [l.slice(0, i), l.slice(i + 1).replace(/^["']|["']$/g, "")]; }),
);
const KEY_ID = env.ASC_KEY_ID, ISSUER = env.ASC_ISSUER_ID;
if (!KEY_ID || !ISSUER) { console.error("scripts/asc-credentials.env is missing ASC_KEY_ID / ASC_ISSUER_ID"); process.exit(1); }
const P8 = env.ASC_P8_PATH || `${os.homedir()}/.appstoreconnect/private_keys/AuthKey_${KEY_ID}.p8`;
if (!fs.existsSync(P8)) { console.error(`private key not found: ${P8}`); process.exit(1); }

// ES256, JOSE-encoded. Node's default is DER — 'ieee-p1363' is the r||s form JWT actually requires,
// and getting this wrong yields a 401 that looks like a bad key rather than a bad signature.
const b64 = (o) => Buffer.from(typeof o === "string" ? o : JSON.stringify(o)).toString("base64url");
const token = (() => {
  const body = `${b64({ alg: "ES256", kid: KEY_ID, typ: "JWT" })}.${b64({
    iss: ISSUER, exp: Math.floor(Date.now() / 1000) + 900, aud: "appstoreconnect-v1",
  })}`;
  const sig = crypto.sign("sha256", Buffer.from(body),
    { key: fs.readFileSync(P8), dsaEncoding: "ieee-p1363" }).toString("base64url");
  return `${body}.${sig}`;
})();

const api = async (path, init = {}) => {
  const r = await fetch(`https://api.appstoreconnect.apple.com${path}`, {
    ...init,
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json", ...(init.headers || {}) },
  });
  if (r.status === 204) return null;
  const j = await r.json().catch(() => ({}));
  if (!r.ok) {
    const detail = (j.errors || []).map((e) => `${e.status} ${e.title}: ${e.detail}`).join("\n  ") || r.statusText;
    throw new Error(`${init.method || "GET"} ${path}\n  ${detail}`);
  }
  return j;
};

const app = (await api(`/v1/apps?filter[bundleId]=${BUNDLE_ID}`)).data[0];
if (!app) { console.error(`no app for bundleId ${BUNDLE_ID}`); process.exit(1); }

// WAIT FOR THE BUILD TO APPEAR, not just to finish processing. altool returns success the moment
// the bytes land, but App Store Connect takes a little longer to register the build in its API —
// so release.sh calling this immediately after upload asked for a build that did not exist yet and
// aborted the release before the web deploy. Observed on 452, minutes after 451 worked, because the
// upload was 10s faster. Polling for existence is the fix; failing fast here was never right.
const want = arg("build");
const findBuild = async () => {
  const builds = (await api(`/v1/builds?filter[app]=${app.id}&sort=-version&limit=10`)).data;
  return { builds, hit: want ? builds.find((b) => b.attributes.version === String(want)) : builds[0] };
};
let { builds, hit: build } = await findBuild();
for (let i = 0; !build && !LIST_ONLY && want && i < 20; i++) {
  if (i === 0) console.log(`  …build ${want} has not appeared in App Store Connect yet; waiting`);
  await new Promise((r) => setTimeout(r, 15_000));
  ({ builds, hit: build } = await findBuild());
}
if (!build) { console.error(`build ${want} never appeared. Recent: ${builds.map((b) => b.attributes.version).join(", ")}`); process.exit(1); }

const groups = (await api(`/v1/betaGroups?filter[app]=${app.id}&limit=50`)).data;
const only = arg("groups");
const ALLOW_EXTERNAL = process.argv.includes("--allow-external");
const targets = selectTargets(groups, { only, allowExternal: ALLOW_EXTERNAL });
const isExternal = (g) => g.attributes.isInternalGroup === false;
const heldBack = groups.filter((g) => isExternal(g) && !targets.includes(g));

console.log(`app      ${app.attributes.name} (${BUNDLE_ID})`);
console.log(`build    ${build.attributes.version} — ${build.attributes.processingState}`);
console.log(`groups   ${groups.map((g) => `${g.attributes.name}${isExternal(g) ? " (external)" : ""}`).join(", ") || "(none)"}`);
console.log(`targets  ${targets.map((g) => g.attributes.name).join(", ") || "(none)"}`);
if (heldBack.length) console.log(`held     ${heldBack.map((g) => g.attributes.name).join(", ")} — external, NOT attached (standing rule; --groups <name> --allow-external to override deliberately)`);
// The TestFlight public link is what the choir actually taps. Show it for every group that has one, so
// "give me the link" is a --list away instead of a click-path through App Store Connect.
for (const g of groups) {
  const a = g.attributes;
  if (a.publicLinkEnabled && a.publicLink) console.log(`link     ${a.name}: ${a.publicLink}${a.publicLinkLimitEnabled ? ` (limit ${a.publicLinkLimit})` : ""}`);
  else if (isExternal(g)) console.log(`link     ${a.name}: public link NOT enabled (App Store Connect → TestFlight → ${a.name} → Enable Public Link)`);
}

if (LIST_ONLY) process.exit(0);

// A build cannot be attached while Apple is still processing it. Poll rather than fail: the wait is
// typically a few minutes and the alternative is remembering to come back and re-run this.
let state = build.attributes.processingState;
for (let i = 0; state === "PROCESSING" && i < 40; i++) {
  await new Promise((r) => setTimeout(r, 30_000));
  state = (await api(`/v1/builds/${build.id}`)).data.attributes.processingState;
  console.log(`  …processing (${(i + 1) * 30}s elapsed) — ${state}`);
}
if (state !== "VALID") { console.error(`build ${build.attributes.version} is ${state}, not VALID — not distributing`); process.exit(1); }

const landed = [], automatic = [], failed = [];
for (const g of targets) {
  const name = g.attributes.name;
  try {
    await api(`/v1/betaGroups/${g.id}/relationships/builds`, {
      method: "POST", body: JSON.stringify({ data: [{ type: "builds", id: build.id }] }),
    });
    landed.push(name);
    console.log(`  ✅ ${name}`);
  } catch (e) {
    // INTERNAL GROUPS TAKE EVERY BUILD AUTOMATICALLY, so assigning one is rejected with a 422 that
    // reads like a failure and is not: internal testers already have it. Reported separately rather
    // than counted as a success, because a summary that claims a group it never touched is how you
    // come to believe the choir has a build they cannot see.
    if (/internal group/i.test(e.message)) { automatic.push(name); console.log(`  ○  ${name} — internal, gets every build automatically`); }
    else if (/already/i.test(e.message)) { landed.push(name); console.log(`  ✅ ${name} (already had it)`); }
    else { failed.push(name); console.log(`  ⚠️  ${name}: ${e.message.split("\n")[1]?.trim() || e.message}`); }
  }
}
const v = build.attributes.version;
if (landed.length) console.log(`\nbuild ${v} assigned to: ${landed.join(", ")}`);

// ATTACHING IS NOT SHIPPING. An EXTERNAL group needs a beta review submission on top of the
// assignment; without it the build sits at "Ready to Submit" forever and no tester can install it,
// while the API reports the group assignment as a clean success. That gap is invisible from here
// and obvious in the App Store Connect UI, which is exactly the kind of step that gets missed.
// This app is already through beta review, so submissions auto-approve — but the submission still
// has to be made. ONLY when an EXTERNAL group was actually attached: a beta review submission is what
// makes a build installable by external testers, so submitting for an internal-only run would be the
// first step of exactly the thing the standing rule forbids.
const landedExternal = targets.some((g) => isExternal(g) && landed.includes(g.attributes.name));
if (landedExternal) {
  try {
    await api("/v1/betaAppReviewSubmissions", {
      method: "POST",
      body: JSON.stringify({ data: { type: "betaAppReviewSubmissions",
        relationships: { build: { data: { type: "builds", id: build.id } } } } }),
    });
    console.log(`build ${v} submitted for beta review`);
  } catch (e) {
    const msg = e.message.split("\n")[1]?.trim() || e.message;
    // Already-submitted is fine; anything else means the build is NOT reaching testers.
    if (/already|state/i.test(msg)) console.log(`build ${v} beta review: ${msg}`);
    else { console.log(`build ${v} BETA REVIEW SUBMISSION FAILED: ${msg}`); process.exitCode = 1; }
  }
}

// READ BACK THE STATE APPLE ACTUALLY HAS. Every step above can return success while the build still
// reaches nobody — that is precisely what happened on 451: the group assignment was a clean 2xx and
// the build sat at "Ready to Submit" until someone looked at the UI. externalBuildState is the exact
// field that column renders, so this is the same fact the screenshot showed, checked instead of
// assumed. A tool that reports its own POSTs rather than the resulting state is how the gap hid.
const detail = await api(`/v1/builds/${build.id}/buildBetaDetail`).catch(() => null);
const ext = detail?.data?.attributes?.externalBuildState;
// EXPLICIT CLASSIFICATION, and an unknown state is NOT a pass. The first version of this map
// omitted IN_BETA_TESTING — the actual success value — which would have reported the shipped build
// as "unknown" and, had it defaulted to OK, would have reproduced the very bug it exists to catch.
const SHIPPED = { IN_BETA_TESTING: 'live to testers', READY_FOR_BETA_TESTING: 'live to testers — "Testing"' };
const PENDING = { WAITING_FOR_BETA_REVIEW: "submitted, waiting for review", IN_BETA_REVIEW: "in review" };
const BROKEN  = {
  READY_FOR_BETA_SUBMISSION: 'NOT SHIPPED — "Ready to Submit"; testers cannot install it',
  BETA_REJECTED: "REJECTED by beta review",
  EXPIRED: "expired",
};
const label = SHIPPED[ext] || PENDING[ext] || BROKEN[ext];
if (!landedExternal) {
  // No external group was targeted, so "Ready to Submit" is the CORRECT external state — it means the
  // choir cannot see this build yet, which is the rule. Internal testers receive every build via
  // Apple's automatic distribution; do not fail the release over an external state we never wanted.
  console.log(`\nexternal state: ${ext || "unknown"} — external groups deliberately not attached`);
  console.log(`✅ build ${v} is uploaded and available to INTERNAL testers only. The choir does not have it.`);
} else {
  console.log(`\nexternal state: ${ext || "unknown"} — ${label || "UNRECOGNISED — check App Store Connect"}`);
  if (ext in SHIPPED) console.log(`✅ build ${v} is reaching EXTERNAL testers (opted in with --allow-external).`);
  else if (ext in PENDING) console.log(`⏳ build ${v} is submitted; it will reach external testers when review clears.`);
  else { console.log(`❌ build ${v} is NOT reaching the external testers you opted into.`); process.exitCode = 1; }
}
if (automatic.length) console.log(`build ${v} reaches automatically: ${automatic.join(", ")}`);
if (failed.length) { console.log(`build ${v} FAILED for: ${failed.join(", ")}`); process.exitCode = 1; }
