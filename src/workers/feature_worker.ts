/**
 * Runs periodically (cron) to:
 * - compute product popularity aggregates from interactions
 * - compute merchant popularity
 * - update FeatureStore entries and Redis global top-K
 *
 * This runs in batches and writes compact JSON to Redis and DB.
 */
import { prisma } from '../lib/prisma';
import redis, { KEYS } from '../lib/redis';

async function computeProductPopularity(batchSize = 10000) {
  // simple aggregated score: clicks * 1 + cart * 3 + purchase * 8
  // We'll aggregate recent window (last 30 days)
  const since = new Date(Date.now() - 30 * 24 * 3600 * 1000);
  // Use raw query for performance: group by productId
  const rows: { productid: string; score: number }[] = await prisma.$queryRawUnsafe(`
    SELECT "productId" as productid,
      SUM(
        CASE "type"
          WHEN 'VIEW' THEN 0.5
          WHEN 'CLICK' THEN 1
          WHEN 'CART' THEN 3
          WHEN 'PURCHASE' THEN 8
          ELSE 0
        END * "value"
      ) as score
    FROM "Interaction"
    WHERE "createdAt" >= '${since.toISOString()}'
    GROUP BY "productId"
    ORDER BY score DESC
    LIMIT 50000;
  `);

  // update product.popularity (batch)
  for (const r of rows) {
    await prisma.product.update({
      where: { id: r.productid },
      data: { popularity: r.score }
    });
    // update redis global score zset
    await redis.zadd(KEYS.globalTop, r.score.toString(), r.productid);
    // update product meta hash
    const meta = await prisma.product.findUnique({ where: { id: r.productid }, select: { id: true, title: true, merchantId: true, productCategoryId: true, popularity: true }});
    if (meta) {
      await redis.hset(KEYS.productMetaHash, meta.id, JSON.stringify({
        title: meta.title,
        merchantId: meta.merchantId,
        productCategoryId: meta.productCategoryId,
        popularity: meta.popularity
      }));
    }
  }
}

async function computeMerchantPopularity() {
  const rows = await prisma.$queryRawUnsafe(`
    SELECT m.id as merchantid, COALESCE(SUM(i_score),0) AS score FROM "Merchant" m
    LEFT JOIN (
      SELECT p."merchantId" as mid,
        SUM(
          CASE "type" WHEN 'VIEW' THEN 0.5 WHEN 'CLICK' THEN 1 WHEN 'CART' THEN 3 WHEN 'PURCHASE' THEN 8 ELSE 0 END
        ) as i_score
      FROM "Interaction" i
      JOIN "Product" p on p.id = i."productId"
      WHERE i."createdAt" >= now() - interval '30 days'
      GROUP BY p."merchantId"
    ) x on x.mid = m.id
    GROUP BY m.id
    ORDER BY score DESC
    LIMIT 10000;
  `);
  for (const r of rows as any[]) {
    // update merchant popularity in DB
    await prisma.merchant.update({
      where: { id: r.merchantid },
      data: { popularity: r.score || 0 }
    });
  }
}

async function runOnce() {
  console.log('feature_worker: starting batch update');
  await computeProductPopularity();
  await computeMerchantPopularity();
  console.log('feature_worker: finished');
}

if (require.main === module) {
  // run once (cron will call this)
  runOnce().then(() => process.exit(0)).catch((e) => {
    console.error(e);
    process.exit(1);
  });
}
