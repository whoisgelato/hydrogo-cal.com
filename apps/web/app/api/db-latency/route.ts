export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

import { prisma } from '@/calcom/prisma'; // whatever your prisma client path is

export async function GET() {
  const times: number[] = [];
  for (let i = 0; i < 3; i++) {
    const t0 = Date.now();
    try {
      await prisma.$queryRaw`select 1`;
    } catch {}
    times.push(Date.now() - t0);
  }
  return new Response(JSON.stringify({ ms: times, avg: times.reduce((a,b)=>a+b,0)/times.length }), {
    status: 200,
    headers: { 'content-type': 'application/json' },
  });
}
