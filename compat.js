"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.LmdbWrapper = void 0;
exports.open = open;
// @ts-check
const index_1 = require("./index");
class LmdbWrapper {
    constructor(lmdb) {
        this.lmdb = lmdb;
    }
    get(key) {
        return this.lmdb.getSync(key);
    }
    put(key, value) {
        return __awaiter(this, void 0, void 0, function* () {
            if (typeof value === "string") {
                value = Buffer.from(value);
            }
            yield this.lmdb.put(key, value);
        });
    }
    resetReadTxn() { }
}
exports.LmdbWrapper = LmdbWrapper;
function open(directory, openOptions) {
    return new LmdbWrapper(new index_1.Lmdb({
        path: directory,
        asyncWrites: false,
        mapSize: 1024 * 1024 * 1024 * 50,
    }));
}
const defaultExport = { open };
exports.default = defaultExport;
