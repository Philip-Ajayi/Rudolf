/**
 * Greedy re-ranker to enforce merchant/category diversity.
 * Input: sorted list by score (highest first).
 * Output: re-ordered list with constraints:
 * - no more than maxSameMerchantConsecutive consecutive items from same merchant
 * - merchant and category ratio limits
 */

type Item = { id: string; score: number; meta: any };

export function ensureDiversity(items: Item[], opts: {
  maxSameMerchantConsecutive: number;
  maxMerchantTotalRatio: number; // fraction of final list any merchant can have
  maxCategoryTotalRatio: number;
}) {
  if (!items || items.length === 0) return items;
  const N = items.length;
  const maxMerchantTotal = Math.ceil(N * opts.maxMerchantTotalRatio);
  const maxCategoryTotal = Math.ceil(N * opts.maxCategoryTotalRatio);

  const merchantCount = new Map<string, number>();
  const categoryCount = new Map<string, number>();

  const out: Item[] = [];
  const pool = items.slice();

  while (pool.length && out.length < N) {
    // pick top candidate that doesn't violate constraints
    let pickedIndex = -1;
    for (let i = 0; i < pool.length; i++) {
      const cand = pool[i];
      const m = cand.meta.merchantId;
      const c = cand.meta.productCategoryId;
      const mCount = merchantCount.get(m) ?? 0;
      const cCount = categoryCount.get(c) ?? 0;

      const lastK = out.slice(-opts.maxSameMerchantConsecutive);
      const lastViolates = lastK.every((x) => x.meta.merchantId === m) && lastK.length >= opts.maxSameMerchantConsecutive;

      if (lastViolates) continue;
      if (mCount >= maxMerchantTotal) continue;
      if (cCount >= maxCategoryTotal) continue;
      // choose first acceptable (pool is sorted by score)
      pickedIndex = i;
      break;
    }
    if (pickedIndex === -1) {
      // no item satisfies constraints; relax by taking top
      out.push(pool.shift()!);
    } else {
      const [picked] = pool.splice(pickedIndex, 1);
      out.push(picked);
      const m = picked.meta.merchantId;
      const c = picked.meta.productCategoryId;
      merchantCount.set(m, (merchantCount.get(m) ?? 0) + 1);
      categoryCount.set(c, (categoryCount.get(c) ?? 0) + 1);
    }
  }
  return out;
}
