import { randomBytes } from "node:crypto";
import { Lmdb } from "../index";
import { mkdirSync, rmSync } from "node:fs";

const KEY_SIZE = 64;
const ENTRY_SIZE = 64 * 1024; // 64KB
const MAX_TIME = 10000;
const ASYNC_WRITES = true;
const NUM_ENTRIES = Math.floor((1024 * 1024 * 1024) / ENTRY_SIZE); // Total memory used 1GB
const MAP_SIZE = 1024 * 1024 * 1024 * 10;

let key = 0;
function generateEntry() {
  return {
    key: String(key++),
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
    asyncWrites: ASYNC_WRITES,
    mapSize: MAP_SIZE,
  });

  console.log("Generating entries for testing");
  const entries = [...Array(NUM_ENTRIES)].map(() => {
    return generateEntry();
  });
  console.log("Writing entries");
  await safeDB.startWriteTransaction();
  for (let entry of entries) {
    await safeDB.put(entry.key, entry.value);
  }
  await safeDB.commitWriteTransaction();

  console.log("(transaction) Reading all entries out");
  safeDB.startReadTransaction();
  {
    const start = Date.now();
    const readEntries = [];
    let i = 0;
    while (Date.now() - start < MAX_TIME && i < entries.length) {
      readEntries.push(safeDB.getSync(entries[i].key));
      i += 1;
    }
    const duration = Date.now() - start;
    const throughput = readEntries.length / duration;
    console.log(
      "(transaction) Safe Throughput:",
      throughput,
      "entries / second",
    );
  }
  safeDB.commitReadTransaction();

  console.log("(no-transaction) Reading all entries out");
  {
    const start = Date.now();
    const readEntries = [];
    let i = 0;
    while (Date.now() - start < MAX_TIME && i < entries.length) {
      readEntries.push(safeDB.getSync(entries[i].key));
      i += 1;
    }
    const duration = Date.now() - start;
    const throughput = readEntries.length / duration;
    console.log(
      "(no-transaction) Safe Throughput:",
      throughput,
      "entries / second",
    );
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
