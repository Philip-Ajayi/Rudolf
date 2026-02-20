import { prisma } from '../lib/prisma';
import redis, { KEYS } from '../lib/redis';
import { fuzzySearchProductIds } from '../utils/fuzzy';
import { sampleMerchantBoost, recordBanditOutcome } from './bandit';
import { ensureDiversity } from '../utils/diversity';

type FeedParams = {
  userId?: string | null;
  searchText?: string | null;
  productCategoryId?: string | null;
  cursor?: string | null;
  limit?: number;
  sessionId?: string | null;
};

const DEFAULT_LIMIT = 30;
const CANDIDATE_K = 200; // fetch K candidates then rescore

async function getPrecomputedUserTop(userId: string | undefined | null, k = CANDIDATE_K) {
  if (!userId) return [];
  const key = KEYS.userTopK(userId);
  
  const arr = await redis.zrevrange(key, 0, k - 1, 'WITHSCORES');
  const out: { id: string; score: number }[] = [];
  for (let i = 0; i < arr.length; i += 2) {
    out.push({ id: arr[i], score: Number(arr[i + 1]) });
  }
  return out;
}

async function getGlobalTop(k = CANDIDATE_K) {
  const arr = await redis.zrevrange(KEYS.globalTop, 0, k - 1, 'WITHSCORES');
  const out: { id: string; score: number }[] = [];
  for (let i = 0; i < arr.length; i += 2) {
    out.push({ id: arr[i], score: Number(arr[i + 1]) });
  }
  return out;
}

async function loadProductMeta(ids: string[]) {
  if (ids.length === 0) return {};
  const metas = await redis.hmget(KEYS.productMetaHash, ...ids);
  const out: Record<string, any> = {};
  ids.forEach((id, idx) => {
    if (metas[idx]) out[id] = JSON.parse(metas[idx]);
  });
  const missing = ids.filter((id) => !out[id]);
  if (missing.length) {
    const rows = await prisma.product.findMany({
      where: { id: { in: missing } },
      select: {
        id: true,
        title: true,
        merchantId: true,
        productCategoryId: true,
        popularity: true
      }
    });
    for (const r of rows) {
      out[r.id] = {
        title: r.title,
        merchantId: r.merchantId,
        productCategoryId: r.productCategoryId,
        popularity: r.popularity
      };
    
      redis.hset(KEYS.productMetaHash, r.id, JSON.stringify(out[r.id]));
    }
  }
  return out;
}

/**
 * Score composition:
 * final = normalize(sum of weighted components)
 * components:
 *  - precomputedCFScore (from user topk or feature store)  (wCF)
 *  - productPopularity (wPop)
 *  - merchantBoost (from bandit sampling) (wBandit)
 *  - textScore (from fuzzy) (wText)
 *  - sessionAffinity (wSess)
 *  - recency boost for newer items
 */
export async function getFeed(params: FeedParams) {
  const {
    userId,
    searchText,
    productCategoryId,
    cursor,
    limit = DEFAULT_LIMIT,
    sessionId
  } = params;

  const candidatesMap = new Map<string, { baseScore: number }>();

  const userTop = await getPrecomputedUserTop(userId, CANDIDATE_K);
  for (const r of userTop) candidatesMap.set(r.id, { baseScore: r.score });

  
  let textMatches: { id: string; textScore: number }[] = [];
  if (searchText && searchText.trim().length > 0) {
    textMatches = await fuzzySearchProductIds(searchText, 200);
    for (const m of textMatches) {
      const cur = candidatesMap.get(m.id);
      if (cur) cur.baseScore = Math.max(cur.baseScore, 0.05 + m.textScore * 0.8);
      else candidatesMap.set(m.id, { baseScore: 0.05 + m.textScore * 0.8 });
    }
  }


  
  if (candidatesMap.size < limit * 3) {
    const global = await getGlobalTop(200);
    for (const g of global) {
      if (!candidatesMap.has(g.id)) candidatesMap.set(g.id, { baseScore: g.score * 0.6 });
    }
  }

  
  if (productCategoryId) {
    // request DB for products in category if candidates insufficient
    if (candidatesMap.size < limit * 2) {
      const rows = await prisma.product.findMany({
        where: { productCategoryId },
        select: { id: true, popularity: true },
        orderBy: { popularity: 'desc' },
        take: 200
      });
      for (const r of rows) {
        if (!candidatesMap.has(r.id)) candidatesMap.set(r.id, { baseScore: r.popularity * 0.5 });
      }
    }
  }

  
  const candidateIds = Array.from(candidatesMap.keys()).slice(0, CANDIDATE_K);
  const metas = await loadProductMeta(candidateIds);

  const results: { id: string; score: number; meta: any }[] = [];
  for (const id of candidateIds) {
    const meta = metas[id];
    if (!meta) continue;
    const base = candidatesMap.get(id)?.baseScore ?? 0.01;
    const textScore = textMatches.find((t) => t.id === id)?.textScore ?? 0;
    const popularity = typeof meta.popularity === 'number' ? meta.popularity : 0;
    // merchant bandit boost (sample)
    const merchantBoost = await sampleMerchantBoost(meta.merchantId).catch(() => 0.5);
    // session affinity: check session recent list in redis
    let sessionAffinity = 0;
    if (sessionId) {
      const list = await redis.lrange(KEYS.sessionRecent(sessionId), 0, 20);
      if (list.includes(id)) sessionAffinity = 1.0;
    }
    
    const wCF = 0.45;
    const wPop = 0.18;
    const wBandit = 0.12;
    const wText = searchText ? 0.20 : 0.05;
    const wSess = 0.1;

    const finalScore =
      wCF * base +
      wPop * popularity +
      wBandit * merchantBoost +
      wText * textScore +
      wSess * sessionAffinity;

    results.push({ id, score: finalScore, meta });
  }


  const explorationRate = 0.07; // 7% injected
  const finalSorted = results.sort((a, b) => b.score - a.score);

  const reRanked = ensureDiversity(finalSorted, {
    maxSameMerchantConsecutive: 1,
    maxMerchantTotalRatio: 0.25,
    maxCategoryTotalRatio: 0.4
  });

  const page = reRanked.slice(0, Math.max(limit, 50));
  const pageToReturn = page.slice(0, limit);


  const productRows = await prisma.product.findMany({
    where: { id: { in: pageToReturn.map((p) => p.id) } },
    select: {
      id: true,
      title: true,
      description: true,
      merchantId: true,
      productCategoryId: true,
      popularity: true
    }
  });

  const productMap = new Map(productRows.map((r) => [r.id, r]));
  const ordered = pageToReturn.map((p) => ({
    score: p.score,
    product: productMap.get(p.id) ?? { id: p.id, title: p.meta?.title ?? '', description: '' }
  }));


  
  const nextCursor = ordered.length ? ordered[ordered.length - 1].product.id : null;

  return { items: ordered, cursor: nextCursor };
}
