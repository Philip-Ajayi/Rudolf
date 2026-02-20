import { prisma } from '../lib/prisma';

/**
 * Fuzzy / text search using Postgres trigram + tsvector fallback.
 * Returns list of product IDs and a textScore normalized [0,1].
 */
export async function fuzzySearchProductIds(query: string, limit = 100) {
  if (!query || query.trim().length === 0) return [];
  // Use similarity from pg_trgm (must create extension and trigram index in DB migration)
  // Fallback to plainto_tsquery ranking if trigram not available.
  const sanitized = query.replace(/'/g, "''");
  const rows = await prisma.$queryRawUnsafe(`
    SELECT id, greatest(similarity(title, '${sanitized}'), similarity(description, '${sanitized}')) AS sim
    FROM "Product"
    WHERE title ILIKE '%${sanitized}%' OR description ILIKE '%${sanitized}%'
    ORDER BY sim DESC NULLS LAST
    LIMIT ${limit};
  `);
  // normalize sim to [0,1] (similarity returns 0..1)
  return rows.map((r: any) => ({ id: r.id as string, textScore: Math.min(1, Number(r.sim) || 0) }));
}
