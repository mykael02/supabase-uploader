import fs from "fs";
import path from "path";
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import {
  contentTypeFor,
  getEnv,
  toBool,
  toInt,
  writeCsvLine
} from "./utils.js";

const SUPABASE_URL = getEnv("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = getEnv("SUPABASE_SERVICE_ROLE_KEY");
const LOCAL_ROOT = getEnv("LOCAL_ROOT");

const MAX_MB = toInt(getEnv("MAX_MB", "50"), 50);
const MAX_BYTES = MAX_MB * 1024 * 1024;
const SKIP_OVER_BYTES = MAX_BYTES - (256 * 1024);

const UPSERT = toBool(getEnv("UPSERT", "false"), false);
const DRY_RUN = toBool(getEnv("DRY_RUN", "false"), false);

if (!LOCAL_ROOT) throw new Error("Missing LOCAL_ROOT in .env");

// Only require Supabase creds if NOT dry-running
if (!DRY_RUN && (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY)) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env (or set DRY_RUN=true)");
}

const supabase = DRY_RUN
  ? null
  : createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });

function parseCsvLines(csv) {
  const lines = csv.split(/\r?\n/).filter(Boolean);
  lines.shift(); // header
  const out = [];
  for (const line of lines) {
    const cols = [];
    let cur = "", inQ = false;
    for (let i = 0; i < line.length; i++) {
      const ch = line[i];
      if (ch === '"' && line[i + 1] === '"') { cur += '"'; i++; continue; }
      if (ch === '"') { inQ = !inQ; continue; }
      if (ch === "," && !inQ) { cols.push(cur); cur = ""; continue; }
      cur += ch;
    }
    cols.push(cur);
    out.push(cols);
  }
  return out;
}

async function main() {
  const latestPath = "logs/latest.json";
  if (!fs.existsSync(latestPath)) throw new Error("No logs/latest.json found. Run npm run upload first.");

  const { logPath } = JSON.parse(fs.readFileSync(latestPath, "utf-8"));
  if (!fs.existsSync(logPath)) throw new Error(`Log file not found: ${logPath}`);

  const csv = fs.readFileSync(logPath, "utf-8");
  const rows = parseCsvLines(csv);

  const retry = rows
    .filter(r => ["FAIL", "SKIP"].includes(r[1]))
    .map(r => ({
      status: r[1],
      bucket: r[2],
      rel: r[3],
      remote: r[4],
      bytes: Number(r[5] || 0)
    }))
    .filter(x => x.rel);

  console.log(`Retrying ${retry.length} items from: ${logPath}`);
  console.log("DRY_RUN:", DRY_RUN);

  const stamp = new Date().toISOString().replaceAll(":", "-");
  const outLog = path.join("logs", `resume-${stamp}.csv`);
  const logStream = fs.createWriteStream(outLog, { flags: "w" });
  writeCsvLine(logStream, ["timestamp", "status", "bucket", "relative_path", "remote_path", "bytes", "message"]);

  let ok = 0, fail = 0, skip = 0, exists = 0, dry = 0;

  for (const item of retry) {
    const localPath = path.join(LOCAL_ROOT, item.rel);

    if (!fs.existsSync(localPath)) {
      writeCsvLine(logStream, [new Date().toISOString(), "FAIL", item.bucket, item.rel, item.remote, item.bytes, "Local file missing"]);
      fail++;
      continue;
    }

    const stat = fs.statSync(localPath);
    if (stat.size > SKIP_OVER_BYTES) {
      writeCsvLine(logStream, [new Date().toISOString(), "SKIP", item.bucket, item.rel, item.remote, stat.size, `Still over ${MAX_MB}MB`]);
      skip++;
      continue;
    }

    if (DRY_RUN) {
      writeCsvLine(logStream, [new Date().toISOString(), "DRY_RUN", item.bucket, item.rel, item.remote, stat.size, ""]);
      dry++;
      continue;
    }

    const data = fs.readFileSync(localPath);
    const contentType = contentTypeFor(localPath);

    const { error } = await supabase.storage
      .from(item.bucket)
      .upload(item.remote, data, { contentType, upsert: UPSERT });

    if (error) {
      const msg = String(error.message || "Unknown error");
      if (msg.toLowerCase().includes("already exists")) {
        writeCsvLine(logStream, [new Date().toISOString(), "EXISTS", item.bucket, item.rel, item.remote, stat.size, msg]);
        exists++;
      } else {
        writeCsvLine(logStream, [new Date().toISOString(), "FAIL", item.bucket, item.rel, item.remote, stat.size, msg]);
        fail++;
      }
    } else {
      writeCsvLine(logStream, [new Date().toISOString(), "OK", item.bucket, item.rel, item.remote, stat.size, ""]);
      ok++;
    }
  }

  logStream.end();
  console.log("Resume complete:", { ok, dry, exists, fail, skip });
  console.log("Log saved:", outLog);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
