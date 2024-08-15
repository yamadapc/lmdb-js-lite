// @ts-check
const v8 = require("node:v8");
const { Lmdb } = require("./index");

/**
 * @typedef {Object} DBOpenOptions
 * @property {string} name
 * @property {string} encoding
 * @property {boolean} compression
 */

class LmdbWrapper {
  /**
   * @type {Lmdb}
   */
  #lmdb;

  /**
   * @param {Lmdb} lmdb
   */
  constructor(lmdb) {
    this.#lmdb = lmdb;
  }

  /**
   * @param {string} key
   * @returns {Uint8Array}
   */
  get(key) {
    const value = this.#lmdb.getSync(key);
    if (value == null) return value;
    return Buffer.from(value);
  }

  /**
   * @param {string} key
   * @param {Buffer} value
   * @return {Promise<void>}
   */
  async put(key, value) {
    if (typeof value === "string") {
      value = Buffer.from(v8.serialize(value));
    }
    await this.#lmdb.put(key, value);
  }

  resetReadTxn() {}
}

/**
 * @param {string} directory
 * @param {DBOpenOptions} openOptions
 * @returns {LmdbWrapper}
 */
function openDB(directory, openOptions) {
  return new LmdbWrapper(
    new Lmdb({
      path: directory,
      asyncWrites: false,
      mapSize: 1024 * 1024 * 1024 * 50,
    }),
  );
}

exports.open = openDB;
exports.default = {
  open: openDB,
};
