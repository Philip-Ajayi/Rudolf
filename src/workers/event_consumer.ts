/**
 * Lightweight event consumer that handles incoming user events (views/clicks/purchases)
 * - updates session list in redis (recent interactions)
 * - updates bandit alpha/beta for merchant/category on click/purchase
 * - writes interaction to DB via queue (or direct)
 *
 * In production, events should come via high-throughput stream (Kafka / Kinesis). Here we show the consumer logic.
 */
import redis, { KEYS } from '../lib/redis';
import { recordBanditOutcome } from '../services/bandit';
import { prisma } from '../lib/prisma';

// Example event object:
// { userId, sessionId, productId, type: 'CLICK'|'VIEW'|'PURCHASE' }

async function handleEvent(e: any) {
  const { userId, sessionId, productId, type } = e;
  // 1) Append to session recent list (left push, keep 50)
  if (sessionId) {
    const key = KEYS.sessionRecent(sessionId);
    await redis.lpush(key, productId);
    await redis.ltrim(key, 0, 49);
    await redis.expire(key, 60 * 60 * 24); // 1 day
  }
  // 2) Update bandit counts: for clicks/purchases treat as success, views as failure in simple model
  try {
    const pMeta = await prisma.product.findUnique({ where: { id: productId }, select: { merchantId: true, productCategoryId: true }});
    if (pMeta) {
      const merchantKey = KEYS.merchantBeta(pMeta.merchantId);
      const categoryKey = KEYS.categoryBeta(pMeta.productCategoryId);
      if (type === 'CLICK' || type === 'PURCHASE') {
        await recordBanditOutcome(merchantKey, true);
        await recordBanditOutcome(categoryKey, true);
      } else if (type === 'VIEW') {
        await recordBanditOutcome(merchantKey, false);
        await recordBanditOutcome(categoryKey, false);
      }
    }
  } catch (err) {
    console.error('bandit update failed', err);
  }

  // 3) Persist minimal interaction to DB asynchronously (fast)
  try {
    await prisma.interaction.create({
      data: {
        userId,
        productId,
        type: type as any,
        value: 1
      }
    });
  } catch (e) {
    console.error('persist interaction failed', e);
  }
}

// If running as standalone, poll a Redis list 'events' (push events there)
async function runLoop() {
  console.log('event_consumer started');
  while (true) {
    try {
      const res = await redis.brpop('events', 1); // block pop for 1s
      if (res) {
        const msg = JSON.parse(res[1]);
        await handleEvent(msg);
      } else {
        // no events, loop; avoid busy loop
        await new Promise((r) => setTimeout(r, 50));
      }
    } catch (err) {
      console.error('event_consumer loop error', err);
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
}

if (require.main === module) {
  runLoop().catch((e) => {
    console.error(e);
    process.exit(1);
  });
}
