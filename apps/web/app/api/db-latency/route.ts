// apps/web/app/api/db-latency/route.ts
export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

import { NextResponse } from 'next/server';

// --- Prisma import (Cal monorepo) ---
// Most Cal setups expose a *default* Prisma client from @calcom/prisma:
import prisma from '@calcom/prisma';

// If your repo changed it to *named* export, replace the line above with:
//   import { prisma } from '@calcom/prisma';

function maskDb(url: string | undefined) {
  if (!url) return null;
  try {
    const u = new URL(url);
    const host = u.hostname;
    const port = u.port || (u.protocol === 'postgresql:' ? '5432' : '');
    return { host, port };
  } catch {
    return null;
  }
}

export async function GET() {
  // warm once (ignores errors; we only care about timings that follow)
  try { await prisma.$queryRaw`select 1`; } catch {}

  const samples: number[] = [];
  for (let i = 0; i < 3; i++) {
    const t0 = Date.now();
    try {
      // trivial round-trip
      await prisma.$queryRaw`select 1`;
    } catch (e) {
      // if it fails, record a big number so you notice
      samples.push(9999);
      continue;
    }
    samples.push(Date.now() - t0);
  }

  const avg = samples.reduce((a, b) => a + b, 0) / samples.length;
  const dbUrl = process.env.DATABASE_URL || '';
  const pooled =
    /\bpgbouncer=true\b/i.test(dbUrl) ||
    /:6543(\/|\?|$)/.test(dbUrl) || // Supabase pooled port
    /\bpooler\b/i.test(dbUrl);

  return NextResponse.json(
    {
      ms: samples,
      avg,
      pooled,
      db: maskDb(dbUrl),
      ts: new Date().toISOString(),
    },
    { headers: { 'cache-control': 'no-store' } },
  );
}

// Optional: allow HEAD pings too (fast warmup)
export async function HEAD() {
  return new Response(null, { status: 204, headers: { 'cache-control': 'no-store' } });
}
