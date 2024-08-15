"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __classPrivateFieldGet = (this && this.__classPrivateFieldGet) || function (receiver, state, kind, f) {
    if (kind === "a" && !f) throw new TypeError("Private accessor was defined without a getter");
    if (typeof state === "function" ? receiver !== state || !f : !state.has(receiver)) throw new TypeError("Cannot read private member from an object whose class did not declare it");
    return kind === "m" ? f : kind === "a" ? f.call(receiver) : f ? f.value : state.get(receiver);
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
var _LMDBCacheSafe_instances, _LMDBCacheSafe_getFilePath;
Object.defineProperty(exports, "__esModule", { value: true });
exports.LMDBCacheSafe = void 0;
const stream = __importStar(require("stream"));
const path_1 = __importDefault(require("path"));
const util_1 = require("util");
const core_1 = require("@parcel/core");
const fs_1 = require("@parcel/fs");
const cache_1 = require("@parcel/cache");
const compat_1 = require("./compat");
const packageJson = require("./package.json");
const pipeline = (0, util_1.promisify)(stream.pipeline);
class LMDBCacheSafe {
    constructor(cacheDir) {
        _LMDBCacheSafe_instances.add(this);
        this.fs = new fs_1.NodeFS();
        this.dir = cacheDir;
        // @ts-expect-error The typescript bindings are wrong
        this.fsCache = new cache_1.FSCache(this.fs, cacheDir);
        this.store = (0, compat_1.open)(cacheDir, {
            name: "parcel-cache",
            encoding: "binary",
            compression: true,
        });
    }
    ensure() {
        return Promise.resolve();
    }
    serialize() {
        return {
            dir: this.dir,
        };
    }
    static deserialize(opts) {
        return new LMDBCacheSafe(opts.dir);
    }
    has(key) {
        return Promise.resolve(this.store.get(key) != null);
    }
    get(key) {
        let data = this.store.get(key);
        if (data == null) {
            return Promise.resolve(null);
        }
        return Promise.resolve((0, core_1.deserialize)(data));
    }
    set(key, value) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.setBlob(key, (0, core_1.serialize)(value));
        });
    }
    getStream(key) {
        return this.fs.createReadStream(path_1.default.join(this.dir, key));
    }
    setStream(key, stream) {
        return pipeline(stream, this.fs.createWriteStream(path_1.default.join(this.dir, key)));
    }
    getBlob(key) {
        let buffer = this.store.get(key);
        return buffer != null
            ? Promise.resolve(buffer)
            : Promise.reject(new Error(`Key ${key} not found in cache`));
    }
    setBlob(key, contents) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.store.put(key, contents);
        });
    }
    getBuffer(key) {
        return Promise.resolve(this.store.get(key));
    }
    hasLargeBlob(key) {
        return this.fs.exists(__classPrivateFieldGet(this, _LMDBCacheSafe_instances, "m", _LMDBCacheSafe_getFilePath).call(this, key, 0));
    }
    // eslint-disable-next-line require-await
    getLargeBlob(key) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.fsCache.getLargeBlob(key);
        });
    }
    // eslint-disable-next-line require-await
    setLargeBlob(key, contents, options) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.fsCache.setLargeBlob(key, contents, options);
        });
    }
    deleteLargeBlob(key) {
        // @ts-expect-error missing in .d.ts
        return this.fsCache.deleteLargeBlob(key);
    }
    refresh() {
        // Reset the read transaction for the store. This guarantees that
        // the next read will see the latest changes to the store.
        // Useful in scenarios where reads and writes are multi-threaded.
        // See https://github.com/kriszyp/lmdb-js#resetreadtxn-void
        this.store.resetReadTxn();
    }
}
exports.LMDBCacheSafe = LMDBCacheSafe;
_LMDBCacheSafe_instances = new WeakSet(), _LMDBCacheSafe_getFilePath = function _LMDBCacheSafe_getFilePath(key, index) {
    return path_1.default.join(this.dir, `${key}-${index}`);
};
(0, core_1.registerSerializableClass)(`${packageJson.version}:LMDBCacheSafe`, LMDBCacheSafe);
