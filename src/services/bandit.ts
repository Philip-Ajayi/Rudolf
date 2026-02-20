import redis, { KEYS } from '../lib/redis';

/**
 * Thompson sampling for merchant/category bandit.
 * Stores (alpha, beta) as ints in Redis hash; uses Beta(alpha,beta) sampling by approximate method:
 * draw two gammas via -ln(U) with shape approximation (we do simple Beta sampling using math).
 *
 * Memory: we store only (a,b) per merchant/category. Extremely compact.
 */

async function getAB(key: string) {
  const data = await redis.hmget(key, 'a', 'b');
  const a = Number(data[0] || 1);
  const b = Number(data[1] || 1);
  return { a, b };
}

function sampleBeta(a: number, b: number) {
  // Use approximate sampling via two gamma draws using -ln(U) * shape for integer shapes.
  // For production, use better sampler or store float moments. This is fast and cheap.
  const ga = -Math.log(Math.random()) * a;
  const gb = -Math.log(Math.random()) * b;
  return ga / (ga + gb);
}

export async function sampleMerchantBoost(merchantId: string) {
  const key = KEYS.merchantBeta(merchantId);
  const { a, b } = await getAB(key);
  return sampleBeta(a, b);
}

export async function recordBanditOutcome(key: string, success: boolean) {
  // key is e.g. KEYS.merchantBeta(...)
  if (success) await redis.hincrby(key, 'a', 1);
  else await redis.hincrby(key, 'b', 1);
}
