import { NextApiRequest, NextApiResponse } from 'next';
import { z } from 'zod';
import { getFeed } from '../../services/recommender';
import redis from '../../lib/redis';

const querySchema = z.object({
  userId: z.string().uuid().optional(),
  sessionId: z.string().optional(),
  productCategoryId: z.string().optional(),
  searchText: z.string().optional(),
  cursor: z.string().optional(),
  limit: z.coerce.number().min(1).max(100).optional()
});

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') return res.status(405).end();
  const parsed = querySchema.safeParse(req.query);
  if (!parsed.success) return res.status(400).json({ error: parsed.error.errors });

  try {
    const r = await getFeed(parsed.data);
    // minimal response: item ids, score, small product fields
    return res.json(r);
  } catch (err: any) {
    console.error('feed error', err);
    return res.status(500).json({ error: 'internal' });
  }
}
