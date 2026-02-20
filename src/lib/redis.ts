import Redis from 'ioredis';

const redis = new Redis(process.env.REDIS_URL || 'redis://127.0.0.1:6379', {
  maxRetriesPerRequest: 1,
  enableOfflineQueue: false,
  lazyConnect: false,
});

// Key conventions
export const KEYS = {
  userTopK: (userId: string) => `user:topk:${userId}`, // ZSET productId -> score
  productFactors: (productId: string) => `pf:${productId}`, // hash or json string
  userFactors: (userId: string) => `uf:${userId}`,
  merchantBeta: (merchantId: string) => `bandit:merchant:${merchantId}`, // store alpha,beta
  categoryBeta: (catId: string) => `bandit:category:${catId}`,
  sessionRecent: (sessionId: string) => `session:${sessionId}:recent`, // list of productIds
  globalTop: 'global:topk', // ZSET
  productMetaHash: 'product:meta', // hash productId -> JSON
};

export default redis;
