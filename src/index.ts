import Redis, { RedisOptions } from 'ioredis';

import type { Cache, Config, Store } from 'cache-manager';

export type RedisCache = Cache<RedisStore>;

export interface RedisStore extends Store {
  readonly isCacheable: (value: unknown) => boolean;

  get client(): Redis;
}

const getVal = (value: unknown) => JSON.stringify(value) || '"undefined"';

export class NoCacheableError implements Error {
  name = 'NoCacheableError';

  constructor(public message: string) {}
}

export const avoidNoCacheable = async <T>(p: Promise<T>) => {
  try {
    return await p;
  } catch (e) {
    if (!(e instanceof NoCacheableError)) throw e;
  }
};

async function batchDeletionKeysByPattern(redis: Redis, key: string) {
  const stream = redis.scanStream({
    match: key,
  });

  stream.on('data', function (resultKeys) {
    if (resultKeys.length) {
      redis.unlink(resultKeys);
    }
  });
}

function builder(
  redisCache: Redis,
  reset: () => Promise<void>,
  keys: (pattern: string) => Promise<string[]>,
  options?: Config,
) {
  const isCacheable =
    options?.isCacheable || ((value) => value !== undefined && value !== null);

  return {
    async get<T>(key: string) {
      const val = await redisCache.get(key);
      if (val === undefined || val === null) return undefined;
      else return JSON.parse(val) as T;
    },
    async set(key, value, ttl) {
      if (!isCacheable(value))
        throw new NoCacheableError(`"${value}" is not a cacheable value`);
      const t = ttl === undefined ? options?.ttl : ttl;
      if (t !== undefined && t !== 0)
        await redisCache.set(key, getVal(value), 'PX', t);
      else await redisCache.set(key, getVal(value));
    },
    async mset(args, ttl) {
      const t = ttl === undefined ? options?.ttl : ttl;
      if (t !== undefined && t !== 0) {
        const multi = redisCache.multi();
        for (const [key, value] of args) {
          if (!isCacheable(value))
            throw new NoCacheableError(
              `"${getVal(value)}" is not a cacheable value`,
            );
          multi.set(key, getVal(value), 'PX', t);
        }
        await multi.exec();
      } else
        await redisCache.mset(
          args.flatMap(([key, value]) => {
            if (!isCacheable(value))
              throw new Error(`"${getVal(value)}" is not a cacheable value`);
            return [key, getVal(value)] as [string, string];
          }),
        );
    },
    mget: (...args) =>
      redisCache
        .mget(args)
        .then((x) =>
          x.map((x) =>
            x === null || x === undefined
              ? undefined
              : (JSON.parse(x) as unknown),
          ),
        ),
    async mdel(...args) {
      await redisCache.del(args);
    },
    async del(key) {
      if (key.includes('*')) {
        await batchDeletionKeysByPattern(redisCache, key);
      }

      await redisCache.del(key);
    },
    ttl: async (key) => redisCache.pttl(key),
    keys: (pattern = '*') => keys(pattern),
    reset,
    isCacheable,
    get client() {
      return redisCache;
    },
  } as RedisStore;
}

export async function redisStore(options?: RedisOptions & Config) {
  options ||= {};
  const redisCache = new Redis(options);

  return redisInsStore(redisCache, options);
}

export function redisInsStore(redisCache: Redis, options?: Config) {
  const reset = async () => {
    await redisCache.flushdb();
  };
  const keys = (pattern: string) => redisCache.keys(pattern);

  return builder(redisCache, reset, keys, options);
}
