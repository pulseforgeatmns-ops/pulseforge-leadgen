'use strict';

// Field intelligence can fail independently of Penny's account evidence.
// Reuse its Ads read when present; otherwise load the selected account directly.
async function resolveLivingGoogleAds({ clientId, pool, operatorBrief, loadGoogleAds }) {
  if (operatorBrief?.googleAds) return operatorBrief.googleAds;
  const load = loadGoogleAds || require('../packages/penny-paid-acquisition/googleAdsOperatorBrief').loadGoogleAdsOperatorBriefSection;
  try { return await load({ tenantId: clientId, pool }); }
  catch (_) { return { live: { available: false } }; }
}

// Read-only, tenant-scoped facts for Max's opening brief. Unavailable is never zero.
async function buildLivingCommandBrief({ clientId, pool, googleAds, now = new Date() }) {
  const tenantId = Number(clientId);
  if (!Number.isSafeInteger(tenantId) || tenantId <= 0) throw new Error('An active workspace is required');
  const results = await Promise.allSettled([
    pool.query(`
      SELECT COUNT(*)::int AS total,
        COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '24 hours')::int AS new_24h,
        COUNT(*) FILTER (WHERE status IN ('warm', 'hot') AND COALESCE(setter_status, '') NOT IN ('booked', 'closed'))::int AS warm,
        COUNT(*) FILTER (WHERE setter_status = 'booked' AND COALESCE(status, '') != 'closed')::int AS booked
      FROM prospects
      WHERE client_id = $1 AND COALESCE(do_not_contact, false) = false
        AND COALESCE(status, 'cold') NOT IN ('dead', 'disqualified', 'closed')
        AND COALESCE(setter_status, '') != 'closed'
    `, [tenantId]),
    pool.query("SELECT COUNT(*)::int AS pending FROM pending_comments WHERE client_id = $1 AND status = 'pending'", [tenantId]),
  ]);
  const [pipeline, approvals] = results.map(result => result.status === 'fulfilled' ? result.value.rows[0] : null);
  return {
    clientId: tenantId,
    generatedAt: now.toISOString(),
    pipeline: pipeline ? {
      total: Number(pipeline.total), new24h: Number(pipeline.new_24h),
      warm: Number(pipeline.warm), booked: Number(pipeline.booked),
      windowLabel: 'Last 24 hours',
    } : null,
    approvals: approvals ? { pending: Number(approvals.pending) } : null,
    googleAds: googleAds?.live || null,
  };
}

module.exports = { buildLivingCommandBrief, resolveLivingGoogleAds };
