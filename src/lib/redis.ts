/**
 * Redis
 */

/**
 * imports: externals
 */

import Logger from "@sha3/logger";
import { createClient, RedisClientType } from "redis";
import snappy from "snappy";

/**
 * imports: internals
 */

/**
 * module: initializations
 */

const logger = new Logger("redis");

/**
 * types
 */

export type RedisOptions = {
  url?: string;
  database?: number;
  logging?: boolean | null;
  cacheKeyPreffix?: string;
  defaultExpInSeconds?: number | null;
  enableCompression?: boolean;
};

export type RedisSetOptions = {
  expInSeconds?: number;
};

/**
 * consts
 */

const DEFAULT_EXP_S = 24 * 60 * 60 * 1000;

/**
 * exports
 */

export default class Redis {
  /**
   * private: attributes
   */

  private client: RedisClientType | null = null;

  private defaultExpInSeconds: number = DEFAULT_EXP_S;

  /**
   * private: properties
   */
  /**
   * public: properties
   */
  /**
   * private static: methods
   */
  /**
   * private: methods
   */

  private checkConnection() {
    if (this.options.url) {
      if (!this.client || !this.client.isOpen) {
        throw new Error("redis connection not established");
      } else {
        return true;
      }
    } else {
      if (this.options.logging) {
        logger.debug(`redis bypassed (no url found)`);
      }
      return false;
    }
  }

  private buildCacheKey(key: string, cacheKeyPreffix?: string) {
    if (!cacheKeyPreffix) {
      return this.options.cacheKeyPreffix
        ? `${this.options.cacheKeyPreffix}:${key}`
        : key;
    }
    return `${cacheKeyPreffix}:${key}`;
  }

  private async _set(
    cacheKey: string,
    value: string,
    options: RedisSetOptions
  ) {
    if (value && this.options.enableCompression) {
      try {
        const buffer = await snappy.compress(value);
        const base64Value = buffer.toString("base64");
        value = base64Value;
        logger.debug(`compression: ${value.length} => ${base64Value.length}`);
      } catch (e: any) {
        logger.error(`error compressing redis value: ${e.message}`);
      }
    }
    await this.client!.set(cacheKey, value, {
      EX: options.expInSeconds || this.defaultExpInSeconds,
    });
    if (this.options.logging) {
      logger.debug(`set => ${cacheKey} (${value?.length || 0})`);
    }
  }

  private async _get(cacheKey: string) {
    let result = await this.client!.get(cacheKey);
    if (result && this.options.enableCompression) {
      try {
        const buffer = Buffer.from(result, "base64");
        result = (await snappy.uncompress(buffer, {
          asBuffer: false,
        })) as string;
      } catch (e: any) {
        result = null;
        logger.error(`error uncompressing redis value: ${e.message}`);
      }
    }
    if (this.options.logging) {
      logger.debug(`get => ${cacheKey} (${result?.length || 0})`);
    }
    return result;
  }

  private async _exists(cacheKey: string) {
    const result = await this.client!.exists(cacheKey);
    return result === 1;
  }

  private async _setMultiJSON(hash: Record<string, string>) {
    if (this.options.enableCompression) {
      try {
        const compressed = await Promise.all(
          Object.keys(hash).map((key) => snappy.compress(hash[key]))
        );
        Object.keys(hash).forEach((key, index) => {
          hash[key] = compressed[index].toString("base64");
        });
      } catch (e: any) {
        logger.error(`error compressing redis value: ${e.message}`);
      }
    }
    await this.client!.mSet(hash);
    if (this.options.logging) {
      const size = Object.keys(hash).reduce(
        (prev, current) => prev + (hash[current]?.length || 0),
        0
      );
      logger.debug(`get multi => [${Object.keys(hash).length}] (${size})`);
    }
  }

  private async _getMultiJSON(cacheKeys: string[]) {
    let result = await this.client!.mGet(cacheKeys);
    if (result && this.options.enableCompression) {
      try {
        result = (await Promise.all(
          result.map((i: any) =>
            i
              ? snappy.uncompress(Buffer.from(i, "base64"), { asBuffer: false })
              : null
          )
        )) as (string | null)[];
      } catch (e: any) {
        result = [];
        logger.error(`error uncompressing redis value: ${e.message}`);
      }
    }
    if (this.options.logging) {
      const size = result.reduce(
        (prev, current) => prev + (current?.length || 0),
        0
      );
      logger.debug(`get multi => [${cacheKeys.length}] (${size})`);
    }
    return result;
  }

  /**
   * constructor
   */

  constructor(private options: RedisOptions) {
    if (options.url) {
      this.client = createClient({
        url: this.options.url,
        database: this.options.database || 0,
      });
    }
    if (options.defaultExpInSeconds) {
      this.defaultExpInSeconds = options.defaultExpInSeconds;
    }
  }

  /**
   * public: methods
   */

  public async connect() {
    if (!this.client) {
      throw new Error("can't connect to redis: url not found");
    } else {
      await this.client.connect();
      if (this.options.logging) {
        logger.debug("connected to redis");
      }
    }
  }

  public async time() {
    if (this.checkConnection()) {
      const result = await this.client!.time();
      return result.toISOString();
    }
    return null;
  }

  public async flush() {
    if (this.checkConnection()) {
      await this.client!.flushDb();
    }
  }

  public async set(
    key: string,
    value: any,
    options: { expInSeconds?: number; baseCacheKey?: string } = {}
  ) {
    const cacheKey = this.buildCacheKey(key, options.baseCacheKey);
    if (this.checkConnection()) {
      if ([null, undefined, ""].includes(value)) {
        await this.client!.del(cacheKey);
        if (this.options.logging) {
          logger.debug(`del => ${cacheKey}`);
        }
      } else if (typeof value === "string") {
        await this._set(cacheKey, value, options);
      } else {
        await this._set(cacheKey, JSON.stringify(value), options);
      }
    }
  }

  public async setMultiJSON(
    cachePreffix: string,
    idsAndValues: Record<string, any>
  ) {
    if (this.checkConnection()) {
      if (Object.keys(idsAndValues)?.length) {
        const hashToStore: Record<string, string> = {};
        Object.keys(idsAndValues).forEach((key) => {
          hashToStore[this.buildCacheKey(`${cachePreffix}:${key}`)] =
            JSON.stringify(idsAndValues[key]);
        });
        await this._setMultiJSON(hashToStore);
      }
    }
  }

  public async getJSON<T>(
    key: string,
    options: { baseCacheKey?: string } = {}
  ) {
    const cacheKey = this.buildCacheKey(key, options.baseCacheKey);
    if (this.checkConnection()) {
      const result = await this._get(cacheKey);
      return result ? (JSON.parse(result) as T) : null;
    }
    return null;
  }

  public async getMultiJSON<T>(cachePreffix: string, ids: (string | number)[]) {
    if (this.checkConnection()) {
      if (ids?.length) {
        const keysToRetrieve = ids.map((i: any) =>
          this.buildCacheKey(`${cachePreffix}:${i}`)
        );
        const result = await this._getMultiJSON(keysToRetrieve);
        return result.map((i: any) => JSON.parse(i) as T);
      }
    }
    return [];
  }

  public async getString(key: string): Promise<string | null> {
    const cacheKey = this.buildCacheKey(key);
    if (this.checkConnection()) {
      const result = await this._get(cacheKey);
      return result;
    }
    return null;
  }

  public async exists(key: string) {
    const cacheKey = this.buildCacheKey(key);
    if (this.checkConnection()) {
      const result = await this._exists(cacheKey);
      return result;
    }
    return false;
  }

  public async increment(key: string, options: { baseCacheKey?: string } = {}) {
    const cacheKey = this.buildCacheKey(key, options.baseCacheKey);
    if (this.checkConnection()) {
      const value: number = await this.client!.incr(cacheKey);
      if (this.options.logging) {
        logger.debug(`incr => ${cacheKey}`);
      }
      return value;
    }
  }

  public async expire(
    key: string,
    options: { expInSeconds: number; baseCacheKey?: string }
  ) {
    const cacheKey = this.buildCacheKey(key, options.baseCacheKey);
    if (this.checkConnection()) {
      const { expInSeconds } = options;
      await this.client!.expireAt(
        cacheKey,
        this.addSeconds(new Date(), expInSeconds)
      );
      if (this.options.logging) {
        logger.debug(`expireAt => ${cacheKey}`);
      }
    }
  }

  private addSeconds(value: Date, seconds: number): Date {
    const result: Date = new Date(value.getTime());
    result.setSeconds(result.getSeconds() + seconds);
    return result;
  }
}
