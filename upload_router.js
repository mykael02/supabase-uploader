import fs from "fs";
import path from "path";
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import {
  buildRouter,
  contentTypeFor,
  getEnv,
  stableIdForString,
  toBool,
  toInt,
  toPosix,
  walk,
  writeCsvLine
} from "./utils.js";

const SUPABASE_URL = getEnv("SUPABASE_URL");
const SUPABASE_SERVICE_ROLE_KEY = getEnv("SUPABASE_SERVICE_ROLE_KEY");

const LOCAL_ROOT = getEnv("LOCAL_ROOT");
const REMOTE_PREFIX = getEnv("REMOTE_PREFIX", "upload");

const MAX_MB = toInt(getEnv("MAX_MB", "50"), 50);
const MAX_BYTES = MAX_MB * 1024 * 1024;
const SKIP_OVER_BYTES = MAX_BYTES - (256 * 1024); // safety buffer

const UPSERT = toBool(getEnv("UPSERT", "false"), false);
const DRY_RUN = toBool(getEnv("DRY_RUN", "false"), false);

const buckets = {
  people: getEnv("BUCKET_PEOPLE", "archive-people"),
  performance: getEnv("BUCKET_PERFORMANCE", "archive-performance"),
  product: getEnv("BUCKET_PRODUCT", "archive-product"),
};

if (!LOCAL_ROOT) throw new Error("Missing LOCAL_ROOT in .env");

// Only require Supabase creds if NOT dry-running
if (!DRY_RUN && (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY)) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env (or set DRY_RUN=true)");
}

const supabase = DRY_RUN
  ? null
  : createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, { auth: { persistSession: false } });

const routeBucket = buildRouter();

function makeRemotePath(localPath, bucket) {
  const rel = path.relative(LOCAL_ROOT, localPath);
  const rootTag = `${REMOTE_PREFIX}-${bucket.replace("archive-", "")}-${stableIdForString(LOCAL_ROOT)}`;
  return toPosix(path.join(rootTag, rel));
}

async function uploadOne(localPath, logStream) {
  const stat = fs.statSync(localPath);
  const size = stat.size;
  const rel = path.relative(LOCAL_ROOT, localPath);
  const sizeMB = size / (1024 * 1024);

  // Enforce < 50MB (or MAX_MB)
  if (size > SKIP_OVER_BYTES) {
    const reason = `SKIP_over_${MAX_MB}MB (${sizeMB.toFixed(1)}MB)`;
    console.warn(`${reason}: ${rel}`);
    writeCsvLine(logStream, [new Date().toISOString(), "SKIP", "", rel, "", size, reason]);
    return { ok: false, skipped: true };
  }

  // Type/folder-based router (may skip)
  const decision = routeBucket({ localPath, relPath: rel, buckets });
  if (decision.action === "skip") {
    const reason = `SKIP_${decision.reason}`;
    console.warn(`${reason}: ${rel}`);
    writeCsvLine(logStream, [new Date().toISOString(), "SKIP", "", rel, "", size, reason]);
    return { ok: false, skipped: true };
  }

  const bucket = decision.bucket;
  const remotePath = makeRemotePath(localPath, bucket);
  const contentType = contentTypeFor(localPath);

  if (DRY_RUN) {
    console.log(`DRY_RUN: [${bucket}] ${rel} -> ${remotePath}`);
    writeCsvLine(logStream, [new Date().toISOString(), "DRY_RUN", bucket, rel, remotePath, size, ""]);
    return { ok: true, dryRun: true };
  }

  const data = fs.readFileSync(localPath);

  const { error } = await supabase.storage
    .from(bucket)
    .upload(remotePath, data, { contentType, upsert: UPSERT });

  if (error) {
    const msg = String(error.message || "Unknown error");
    if (msg.toLowerCase().includes("already exists")) {
      console.log(`EXISTS: [${bucket}] ${rel}`);
      writeCsvLine(logStream, [new Date().toISOString(), "EXISTS", bucket, rel, remotePath, size, msg]);
      return { ok: true, existed: true };
    }
    console.error(`FAIL: [${bucket}] ${rel} -> ${msg}`);
    writeCsvLine(logStream, [new Date().toISOString(), "FAIL", bucket, rel, remotePath, size, msg]);
    return { ok: false };
  }

  console.log(`OK: [${bucket}] ${rel}`);
  writeCsvLine(logStream, [new Date().toISOString(), "OK", bucket, rel, remotePath, size, ""]);
  return { ok: true };
}

async function main() {
  if (!fs.existsSync("logs")) fs.mkdirSync("logs");

  const stamp = new Date().toISOString().replaceAll(":", "-");
  const logPath = path.join("logs", `upload-${stamp}.csv`);

  console.log("LOCAL_ROOT:", LOCAL_ROOT);
  console.log("REMOTE_PREFIX:", REMOTE_PREFIX);
  console.log("MAX_MB:", MAX_MB);
  console.log("UPSERT:", UPSERT);
  console.log("DRY_RUN:", DRY_RUN);
  console.log("Buckets:", buckets);
  if (!DRY_RUN) console.log("SUPABASE_URL:", SUPABASE_URL);
  console.log("Log:", logPath);

  const files = walk(LOCAL_ROOT);
  console.log(`Found ${files.length} files`);

  const logStream = fs.createWriteStream(logPath, { flags: "w" });
  writeCsvLine(logStream, ["timestamp", "status", "bucket", "relative_path", "remote_path", "bytes", "message"]);

  let ok = 0, fail = 0, skip = 0, exists = 0, dry = 0;

  for (const f of files) {
    const res = await uploadOne(f, logStream);
    if (res?.skipped) skip++;
    else if (res?.existed) exists++;
    else if (res?.dryRun) dry++;
    else if (res?.ok) ok++;
    else fail++;
  }

  logStream.end();
  console.log("\nDone.");
  console.log({ ok, dry, exists, fail, skip });
  console.log("Log saved:", logPath);

  fs.writeFileSync("logs/latest.json", JSON.stringify({ logPath }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
