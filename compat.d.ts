import { Lmdb } from "./index";
export function openDB(
  directory: string,
  openOptions: DBOpenOptions,
): LmdbWrapper;
export type DBOpenOptions = {
  name: string;
  encoding: string;
  compression: boolean;
};
/**
 * @typedef {Object} DBOpenOptions
 * @property {string} name
 * @property {string} encoding
 * @property {boolean} compression
 */
declare class LmdbWrapper {
  /**
   * @param {Lmdb} lmdb
   */
  constructor(lmdb: Lmdb);
  /**
   * @param {string} key
   * @returns {Uint8Array}
   */
  get(key: string): Uint8Array;
  /**
   * @param {string} key
   * @param {Buffer} value
   */
  put(key: string, value: Buffer): void;
  resetReadTxn(): void;
}
