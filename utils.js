import fs from "fs";
import path from "path";
import crypto from "crypto";
import mime from "mime-types";

export function getEnv(name, fallback = undefined) {
  const v = process.env[name];
  return (v === undefined || v === "") ? fallback : v;
}

export function toBool(v, fallback = false) {
  if (v === undefined || v === null) return fallback;
  return String(v).toLowerCase() === "true";
}

export function toInt(v, fallback) {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

export function toPosix(p) {
  return p.split(path.sep).join("/");
}

export function stableIdForString(s) {
  return crypto.createHash("sha1").update(String(s)).digest("hex").slice(0, 10);
}

export function walk(dir) {
  const out = [];
  const items = fs.readdirSync(dir, { withFileTypes: true });
  for (const item of items) {
    const full = path.join(dir, item.name);
    if (item.isDirectory()) out.push(...walk(full));
    else if (item.isFile()) out.push(full);
  }
  return out;
}

export function contentTypeFor(localPath) {
  return mime.lookup(localPath) || "application/octet-stream";
}

export function writeCsvLine(stream, cols) {
  const escaped = cols.map(v => {
    const s = String(v ?? "");
    if (s.includes(",") || s.includes('"') || s.includes("\n")) {
      return `"${s.replaceAll('"', '""')}"`;
    }
    return s;
  });
  stream.write(escaped.join(",") + "\n");
}

// ======= ROUTING RULES (EDIT HERE) =======
export function buildRouter() {
  // Media -> PERFORMANCE
  const IMAGE_EXTS = new Set([
    ".jpg", ".jpeg", ".png", ".webp", ".gif", ".tif", ".tiff", ".heic", ".heif", ".bmp"
  ]);

  const VIDEO_EXTS = new Set([
    ".mp4", ".mov", ".m4v", ".avi", ".mkv", ".webm"
  ]);

  // PDFs -> PRODUCT
  const PDF_EXTS = new Set([".pdf"]);

  // Office docs -> PEOPLE
  const DOC_EXTS = new Set([
    ".doc", ".docx", ".rtf", ".txt",
    ".xls", ".xlsx", ".csv", ".ods",
    ".ppt", ".pptx", ".thmx",
    ".msg", ".eml",
    ".pub"
  ]);

  // Creative / CAD / archives -> PRODUCT
  const CREATIVE_SOURCE_EXTS = new Set([
    ".psd", ".ai", ".indd",
    ".dwg", ".dxf",
    ".zip", ".rar", ".7z"
  ]);

  // Skip noise/system/local DBs by default
  const SKIP_EXTS = new Set([
    ".ds_store", ".tmp", ".db"
  ]);

  // Skip files with no extension (toggle if you want)
  const SKIP_NO_EXTENSION = true;

  // Optional: folder-name hints override routing
  const PEOPLE_FOLDER_HINTS = ["client", "clients", "prospect", "prospects"];

  return function routeDecision({ localPath, relPath, buckets }) {
    const ext = path.extname(localPath).toLowerCase();
    const relLower = relPath.toLowerCase();

    // Skip rules
    if (SKIP_NO_EXTENSION && (!ext || ext === "")) {
      return { action: "skip", reason: "no_extension" };
    }
    if (SKIP_EXTS.has(ext)) {
      return { action: "skip", reason: `skip_ext:${ext}` };
    }

    // Folder hint override
    if (PEOPLE_FOLDER_HINTS.some(h => relLower.includes(h))) {
      return { action: "upload", bucket: buckets.people };
    }

    // Routing by type
    if (PDF_EXTS.has(ext)) return { action: "upload", bucket: buckets.product };
    if (IMAGE_EXTS.has(ext) || VIDEO_EXTS.has(ext)) return { action: "upload", bucket: buckets.performance };
    if (CREATIVE_SOURCE_EXTS.has(ext)) return { action: "upload", bucket: buckets.product };
    if (DOC_EXTS.has(ext)) return { action: "upload", bucket: buckets.people };

    // Default fallback
    return { action: "upload", bucket: buckets.people };
  };
}
// ========================================
