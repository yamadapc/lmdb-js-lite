import { randomBytes } from "node:crypto";
import { Lmdb } from "../index";
import { mkdirSync, rmSync } from "node:fs";

const KEY_SIZE = 64;
const ENTRY_SIZE = 64; // 64 bytes
const MAX_TIME = 10000;

function generateEntry() {
  return {
    key: randomBytes(KEY_SIZE).toString(),
    value: randomBytes(ENTRY_SIZE),
  };
}

async function main() {
  rmSync("./databases", {
    recursive: true,
    force: true,
  });
  mkdirSync("./databases", {
    recursive: true,
  });
  const safeDB = new Lmdb({
    path: "./databases/unsafe",
    asyncWrites: false,
  });

  {
    console.log("Generating 1 million entries for testing");
    const entries = [...Array(1e6)].map(() => {
      return generateEntry();
    });
    console.log("(no-batching) Writing entries for", MAX_TIME, "ms");
    const start = Date.now();
    let numEntriesInserted = 0;
    await safeDB.startWriteTransaction();
    while (Date.now() - start < MAX_TIME) {
      const entry = entries.pop();
      if (!entry) break;
      safeDB.putNoConfirm(entry.key, entry.value);
      numEntriesInserted += 1;
    }
    await safeDB.commitWriteTransaction();
    const duration = Date.now() - start;
    const throughput = numEntriesInserted / duration;
    console.log("Throughput:", throughput, "entries / second");
  }

  {
    console.log("Generating 1 million entries for testing");
    const entries = [...Array(1e6)].map(() => {
      return generateEntry();
    });
    console.log("(manual batching) Writing entries for", MAX_TIME, "ms");
    const start = Date.now();
    let numEntriesInserted = 0;
    let batch = [];
    await safeDB.startWriteTransaction();
    while (Date.now() - start < MAX_TIME) {
      const entry = entries.pop();
      if (!entry) break;
      batch.push(entry);
      if (batch.length > 100) {
        await safeDB.putMany(batch);
        numEntriesInserted += batch.length;
        batch = [];
      }
    }
    await safeDB.commitWriteTransaction();
    const duration = Date.now() - start;
    const throughput = numEntriesInserted / duration;
    console.log("Safe Throughput:", throughput, "entries / second");
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
