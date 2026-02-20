import { prisma } from '../lib/prisma';
import redis, { KEYS } from '../lib/redis';

type Vec = Float32Array;

function dot(a: Vec, b: Vec) {
  let s = 0;
  for (let i = 0; i < a.length; i++) s += a[i] * b[i];
  return s;
}

function randVec(dim: number, scale = 0.01) {
  const v = new Float32Array(dim);
  for (let i = 0; i < dim; i++) v[i] = (Math.random() - 0.5) * scale;
  return v;
}

const DIM = Number(process.env.LATENT_DIM || 32);
const EPOCHS = 3;
const LEARNING_RATE = 0.025;
const REG = 0.01;
const TOPK = 200; // per-user precompute

async function loadInteractionsWindow(days = 60, limit = 2_000_000) {
  const since = new Date(Date.now() - days * 24 * 3600 * 1000);
  const rows: { userId: string | null; productId: string; weight: number }[] = await prisma.$queryRawUnsafe(`
    SELECT coalesce("userId",'anon') as "userId", "productId",
      SUM(
        CASE "type" WHEN 'VIEW' THEN 0.5 WHEN 'CLICK' THEN 1 WHEN 'CART' THEN 3 WHEN 'PURCHASE' THEN 8 ELSE 0 END
      ) as weight
    FROM "Interaction"
    WHERE "createdAt" >= '${since.toISOString()}'
    GROUP BY coalesce("userId",'anon'), "productId"
    ORDER BY weight DESC
    LIMIT ${limit};
  `);
  return rows;
}

async function run() {
  console.log('CF worker: loading interactions');
  const interactions = await loadInteractionsWindow(90, 1_000_000);
  console.log('CF worker: interactions loaded', interactions.length);


  const users = new Map<string, Map<string, number>>(); // user -> (product -> weight)
  const productsSet = new Set<string>();
  for (const r of interactions) {
    const u = r.userId || 'anon';
    if (!users.has(u)) users.set(u, new Map());
    users.get(u)!.set(r.productId, r.weight);
    productsSet.add(r.productId);
  }


  const productFactors = new Map<string, Vec>();
  const userFactors = new Map<string, Vec>();

  for (const pid of productsSet) productFactors.set(pid, randVec(DIM));
  for (const uid of users.keys()) userFactors.set(uid, randVec(DIM));

  console.log('CF worker: starting SGD', { users: users.size, products: productsSet.size });
  for (let epoch = 0; epoch < EPOCHS; epoch++) {
    let cnt = 0;
    for (const [uid, pm] of users) {
      const uvec = userFactors.get(uid)!;
      for (const [pid, weight] of pm) {
        const pvec = productFactors.get(pid)!;
        const pred = dot(uvec, pvec);
        const err = weight - pred;
        
        for (let k = 0; k < DIM; k++) {
          const ug = err * pvec[k] - REG * uvec[k];
          const pg = err * uvec[k] - REG * pvec[k];
          uvec[k] += LEARNING_RATE * ug;
          pvec[k] += LEARNING_RATE * pg;
        }
        cnt++;
      }
    }
    console.log(`epoch ${epoch} done, updates ${cnt}`);
  
  }

  // Persist compact vectors to FeatureStore (DB) and redis for hot products
  console.log('CF worker: persisting factors to DB (FeatureStore) in batches');
  const pfEntries = Array.from(productFactors.entries());
  for (let i = 0; i < pfEntries.length; i += 500) {
    const batch = pfEntries.slice(i, i + 500);
    const upserts = batch.map(([pid, vec]) => ({
      key: `product:${pid}`,
      namespace: 'product_factors',
      value: JSON.stringify(Array.from(vec))
    }));
    for (const u of upserts) {
      await prisma.featureStore.upsert({
        where: { key: u.key },
        update: { value: JSON.parse(u.value) },
        create: {
          key: u.key,
          namespace: 'product_factors',
          value: JSON.parse(u.value)
        }
      });
    }
  }

  const ufEntries = Array.from(userFactors.entries());
  for (let i = 0; i < ufEntries.length; i += 200) {
    const batch = ufEntries.slice(i, i + 200);
    for (const [uid, vec] of batch) {
      const key = `user:${uid}`;
      await prisma.featureStore.upsert({
        where: { key },
        update: { value: Array.from(vec) as any },
        create: { key, namespace: 'user_factors', value: Array.from(vec) as any }
      });
    }
  }

  console.log('CF worker: precomputing per-user top-K and writing to Redis');
  for (const [uid, pm] of users) {
    const uvec = userFactors.get(uid)!;
    const scores: [string, number][] = [];
    for (const [pid, pvec] of productFactors) {
      scores.push([pid, dot(uvec, pvec)]);
    }
    scores.sort((a, b) => b[1] - a[1]);
    const top = scores.slice(0, TOPK);
    const key = KEYS.userTopK(uid);
    const args: string[] = [];
    for (const [pid, sc] of top) {
      args.push(sc.toString(), pid);
    }
    if (args.length) await redis.del(key);
    if (args.length) await redis.zadd(key, ...args);
    await redis.expire(key, 24 * 3600); // precomputed for 24h
  }

  console.log('CF worker: done');
}

if (require.main === module) {
  run().then(() => process.exit(0)).catch((e) => {
    console.error('cf_worker error', e);
    process.exit(1);
  });
}
