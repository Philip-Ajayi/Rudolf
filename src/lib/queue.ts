import { Queue, Worker } from 'bullmq';
import Redis from 'ioredis';

const connection = new Redis(process.env.REDIS_URL || 'redis://127.0.0.1:6379');

export const featureQueue = new Queue('feature', { connection });

export function createWorker(name: string, processor: (job: any) => Promise<any>) {
  return new Worker(name, async (job) => processor(job), { connection });
}
