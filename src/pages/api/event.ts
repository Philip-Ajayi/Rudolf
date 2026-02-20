import { NextApiRequest, NextApiResponse } from 'next';
import { z } from 'zod';
import redis from '../../lib/redis';

const schema = z.object({
  userId: z.string().optional(),
  sessionId: z.string(),
  productId: z.string(),
  type: z.enum(['VIEW', 'CLICK', 'CART', 'PURCHASE'])
});

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'POST') return res.status(405).end();

  const parsed = schema.safeParse(req.body);
  if (!parsed.success) return res.status(400).json(parsed.error);

  // push to Redis queue
  await redis.lpush('events', JSON.stringify(parsed.data));

  return res.json({ ok: true });
}
