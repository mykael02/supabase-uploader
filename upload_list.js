import path from "path";
import "dotenv/config";
import { buildRouter, getEnv, walk } from "./utils.js";

const LOCAL_ROOT = getEnv("LOCAL_ROOT");
if (!LOCAL_ROOT) throw new Error("Missing LOCAL_ROOT in .env");

const buckets = {
  people: getEnv("BUCKET_PEOPLE", "archive-people"),
  performance: getEnv("BUCKET_PERFORMANCE", "archive-performance"),
  product: getEnv("BUCKET_PRODUCT", "archive-product"),
};

const routeBucket = buildRouter();

const files = walk(LOCAL_ROOT);

const counts = {
  total: files.length,
  bucket: { [buckets.people]: 0, [buckets.performance]: 0, [buckets.product]: 0, SKIP: 0 },
  ext: {}
};

for (const f of files) {
  const rel = path.relative(LOCAL_ROOT, f);
  const decision = routeBucket({ localPath: f, relPath: rel, buckets });

  if (decision.action === "skip") {
    counts.bucket.SKIP++;
  } else {
    counts.bucket[decision.bucket] = (counts.bucket[decision.bucket] || 0) + 1;
  }

  const ext = path.extname(f).toLowerCase() || "(none)";
  counts.ext[ext] = (counts.ext[ext] || 0) + 1;
}

console.log("LOCAL_ROOT:", LOCAL_ROOT);
console.log("Total files:", counts.total);
console.log("\nBy bucket (including SKIP):");
console.table(counts.bucket);

console.log("\nTop extensions:");
const top = Object.entries(counts.ext).sort((a, b) => b[1] - a[1]).slice(0, 30);
console.table(Object.fromEntries(top));
