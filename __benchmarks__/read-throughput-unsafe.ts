import { randomBytes } from "node:crypto";
import { mkdirSync, rmSync } from "node:fs";
import { open as openLMDBUnsafe } from "lmdb";

const KEY_SIZE = 64;
const ENTRY_SIZE = 64 * 1024; // 64KB
const MAX_TIME = 10000;
const NUM_ENTRIES = Math.floor((1024 * 1024 * 1024) / ENTRY_SIZE); // Total memory used 1GB
const ENABLE_COMPRESSION = true;

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
  const unsafeDB = openLMDBUnsafe({
    path: "./databases/unsafe",
    compression: ENABLE_COMPRESSION,
    encoding: "binary",
    eventTurnBatching: true,
  });

  console.log("Generating entries for testing");
  const entries = [...Array(NUM_ENTRIES)].map(() => {
    return generateEntry();
  });
  console.log("Writing entries");
  await unsafeDB.transaction(async () => {
    for (let entry of entries) {
      await unsafeDB.put(entry.key, entry.value);
    }
  });

  console.log("Reading all entries out");
  {
    const start = Date.now();
    const readEntries = [];
    let i = 0;
    while (Date.now() - start < MAX_TIME && i < entries.length) {
      readEntries.push(unsafeDB.get(entries[i].key));
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
