import { initTracingSubscriber, Lmdb } from "../index.js";
import * as v8 from "node:v8";

beforeAll(() => {
  // initTracingSubscriber();
});

describe("lmdb", () => {
  it("can be opened", () => {
    const db = new Lmdb({
      path: "./test.db",
      asyncWrites: false,
    });
    db.close();
  });

  it("we can put keys and then retrieve them", async () => {
    const db = new Lmdb({
      path: "./test.db",
      asyncWrites: false,
    });
    const value = Math.random().toString();
    await db.put("key", v8.serialize(value));
    const result = await db.get("key");
    const resultValue = v8.deserialize(result);

    console.log({ value, resultValue });
    expect(value).toEqual(resultValue);
  });

  it.only("read and write many entries", async () => {
    const db = new Lmdb({
      path: "./test.db",
      asyncWrites: false,
    });

    await db.startTransaction();
    for (let i = 0; i < 1000; i += 1) {
      await db.put(`${i}`, v8.serialize(i));
    }

    for (let i = 0; i < 1000; i += 1) {
      const result = await db.get(`${i}`);
      const resultValue = v8.deserialize(result);
      expect(resultValue).toEqual(i);
    }
    await db.commitTransaction();
  });
});
