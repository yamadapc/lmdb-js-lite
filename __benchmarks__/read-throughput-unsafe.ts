import { randomBytes } from "node:crypto";
import { mkdirSync, rmSync } from "node:fs";
import { open as openLMDBUnsafe } from "lmdb";
import v8 from "node:v8";

const KEY_SIZE = 64;
const ENTRY_SIZE = 64 * 1024; // 64KB
const MAX_TIME = 10000;
const NUM_ENTRIES = Math.floor((1024 * 1024 * 1024) / ENTRY_SIZE); // Total memory used 1GB
const ENABLE_COMPRESSION = true;

async function main() {
  const unsafeDB = openLMDBUnsafe({
    path: "./databases/read",
    compression: ENABLE_COMPRESSION,
    encoding: "binary",
    eventTurnBatching: true,
  });

  const value = unsafeDB.get("benchmarkInfo");
  if (!value) throw new Error("Run prepare-read-benchmark.ts");
  const benchmarkInfo = v8.deserialize(value);
  console.log(benchmarkInfo);
  const { NUM_ENTRIES } = benchmarkInfo;

  console.log("Reading all entries out");
  {
    const start = Date.now();
    const readEntries = [];
    let i = 0;
    while (Date.now() - start < MAX_TIME) {
      readEntries.push(unsafeDB.get(String(i % NUM_ENTRIES)));
      i += 1;
    }
    const duration = Date.now() - start;
    const throughput = readEntries.length / duration;
    console.log("Unsafe Throughput:", throughput, "entries / second");
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
