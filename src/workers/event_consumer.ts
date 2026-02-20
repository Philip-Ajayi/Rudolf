import redis, { KEYS } from '../lib/redis';
import { recordBanditOutcome } from '../services/bandit';
import { prisma } from '../lib/prisma';


async function handleEvent(e: any) {
  const { userId, sessionId, productId, type } = e;
  if (sessionId) {
    const key = KEYS.sessionRecent(sessionId);
    await redis.lpush(key, productId);
    await redis.ltrim(key, 0, 49);
    await redis.expire(key, 60 * 60 * 24); // 1 day
  }
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
