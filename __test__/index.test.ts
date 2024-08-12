import { initTracingSubscriber, Lmdb } from "../index.js";
import { open as openLMDBUnsafe, type Database as UnsafeDatabase } from "lmdb";
import * as v8 from "node:v8";
import { mkdirSync, rmSync } from "node:fs";

beforeAll(() => {
  initTracingSubscriber();
});

beforeEach(() => {
  rmSync("./databases", {
    recursive: true,
    force: true,
  });
  mkdirSync("./databases", {
    recursive: true,
  });
});

jest.setTimeout(20000);
describe("lmdb", () => {
  let db: Lmdb | null = null;
  const asyncWrites = false;
  const compression = false;
  const numEntriesToTest = 100000;

  afterEach(() => {
    db?.close();
  });

  it("can be opened", () => {
    db = new Lmdb({
      path: "./databases/test.db",
      asyncWrites,
    });
    db.close();
    db = null;
  });

  it("we can put keys and then retrieve them", async () => {
    db = new Lmdb({
      path: "./databases/test.db",
      asyncWrites,
    });
    const value = Math.random().toString();
    await db.put("key", v8.serialize(value));
    const result = await db.get("key");
    const resultValue = v8.deserialize(result);

    expect(value).toEqual(resultValue);
  });

  it("read and write many entries", async () => {
    db = new Lmdb({
      path: "./databases/test.db",
      asyncWrites,
    });

    await db.startTransaction();
    for (let i = 0; i < numEntriesToTest; i += 1) {
      await db.put(`${i}`, v8.serialize(i));
    }
    await db.commitTransaction();

    for (let i = 0; i < numEntriesToTest; i += 1) {
      const result = db.getSync(`${i}`);
      const resultValue = v8.deserialize(result);
      expect(resultValue).toEqual(i);
    }
  });

  describe("reading", () => {
    beforeEach(async () => {
      db = new Lmdb({
        path: "./databases/test.db",
        asyncWrites,
      });

      await db.startTransaction();
      for (let i = 0; i < numEntriesToTest; i += 1) {
        await db.put(`${i}`, v8.serialize(i));
      }
      await db.commitTransaction();
    });

    afterEach(() => {
      db.close();
    });

    it("read many entries, no transaction", async () => {
      for (let i = 0; i < numEntriesToTest; i += 1) {
        const result = await db.get(`${i}`);
        const resultValue = v8.deserialize(result);
        expect(resultValue).toEqual(i);
      }
    });

    it("read many entries, synchronous, no transaction", async () => {
      for (let i = 0; i < numEntriesToTest; i += 1) {
        const result = db.getSync(`${i}`);
        const resultValue = v8.deserialize(result);
        expect(resultValue).toEqual(i);
      }
    });

    describe("unsafe", () => {
      let unsafeDB: UnsafeDatabase | null = null;

      beforeEach(async () => {
        unsafeDB = openLMDBUnsafe({
          path: "./databases/unsafe",
          compression,
        });

        await unsafeDB.transaction(async () => {
          for (let i = 0; i < numEntriesToTest; i += 1) {
            await unsafeDB.put(`${i}`, v8.serialize(i));
          }
        });
      }, 40000);

      it("read many entries", () => {
        for (let i = 0; i < numEntriesToTest; i += 1) {
          const result = unsafeDB.get(`${i}`);
          const resultValue = v8.deserialize(result);
          expect(resultValue).toEqual(i);
        }
      });
    });
  });

  describe("unsafe", () => {
    it("read and write many entries", async () => {
      const unsafeDB = openLMDBUnsafe({
        path: "./databases/unsafe",
        compression,
      });

      await unsafeDB.transaction(async () => {
        for (let i = 0; i < numEntriesToTest; i += 1) {
          await unsafeDB.put(`${i}`, v8.serialize(i));
        }
      });

      for (let i = 0; i < numEntriesToTest; i += 1) {
        const result = unsafeDB.get(`${i}`);
        const resultValue = v8.deserialize(result);
        expect(resultValue).toEqual(i);
      }
    });
  });
});
