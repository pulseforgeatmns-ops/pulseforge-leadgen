'use strict';

const { randomUUID } = require('crypto');
const { calculateRevenue, outcomeStatus } = require('../utils/revenueDomain');
const { assertRevenueFlag, loadRevenueFlags } = require('../utils/revenueFlags');

const ZERO_TOTALS = Object.freeze({
  booked_revenue_cents: 0,
  delivered_revenue_cents: 0,
  collected_revenue_cents: 0,
  refunded_revenue_cents: 0,
  net_collected_revenue_cents: 0,
  job_count: 0,
  payment_count: 0,
  refund_count: 0,
  unattributed_outcome_count: 0,
  disputed_outcome_count: 0,
});

function number(value) { return Number(value || 0); }
function add(total, key, value) { total[key] += number(value); }
function scopeSql(clientId, from, to, alias, parameterStart = 1, occurredExpression = `${alias}.occurred_at`) {
  const values = [];
  const clauses = [];
  if (clientId !== null && clientId !== undefined) { values.push(clientId); clauses.push(`${alias}.client_id = $${parameterStart + values.length - 1}`); }
  if (from) { values.push(from); clauses.push(`${occurredExpression} >= $${parameterStart + values.length - 1}`); }
  if (to) { values.push(to); clauses.push(`${occurredExpression} < $${parameterStart + values.length - 1}`); }
  return { where: clauses.length ? `WHERE ${clauses.join(' AND ')}` : '', values };
}

function totalsFromOutcomes(rows) {
  const totals = { ...ZERO_TOTALS };
  for (const row of rows) {
    add(totals, 'booked_revenue_cents', row.booked_revenue_cents);
    add(totals, 'delivered_revenue_cents', row.delivered_revenue_cents);
    add(totals, 'refunded_revenue_cents', row.refunded_revenue_cents);
    const net = number(row.collected_revenue_cents);
    totals.net_collected_revenue_cents += net;
    totals.collected_revenue_cents += net + number(row.refunded_revenue_cents);
    totals.job_count += 1;
    add(totals, 'payment_count', row.payment_count);
    add(totals, 'refund_count', row.refund_count);
    if (row.attribution_status === 'unattributed') totals.unattributed_outcome_count += 1;
    if (row.attribution_status === 'disputed' || row.outcome_status === 'disputed') totals.disputed_outcome_count += 1;
  }
  return totals;
}

async function sourceRows(db, clientId, from, to) {
  const scope = scopeSql(clientId, from, to, 'j', 1, 'COALESCE(j.actual_end,j.scheduled_start)');
  const { rows } = await db.query(`
    SELECT j.*, o.prospect_id, o.company_id, o.source, o.lead_source_detail,
      o.campaign_id, o.sequence_id, o.attribution_status, o.human_owner,
      o.created_at AS opportunity_created_at,
      COALESCE(p.succeeded, 0)::bigint AS successful_payments_cents,
      COALESCE(p.refunded, 0)::bigint AS refunded_cents,
      p.first_received, COALESCE(p.payment_count, 0)::int AS payment_count,
      COALESCE(p.refund_count, 0)::int AS refund_count
    FROM revenue_jobs j
    LEFT JOIN opportunities o ON o.client_id = j.client_id AND o.id = j.opportunity_id
    LEFT JOIN LATERAL (
      SELECT COALESCE(SUM(amount_cents) FILTER (WHERE status IN ('succeeded','partially_refunded','refunded')),0) AS succeeded,
        COALESCE(SUM(refunded_amount_cents) FILTER (WHERE status IN ('succeeded','partially_refunded','refunded')),0) AS refunded,
        MIN(received_at) FILTER (WHERE status IN ('succeeded','partially_refunded','refunded')) AS first_received,
        COUNT(*) FILTER (WHERE status IN ('succeeded','partially_refunded','refunded')) AS payment_count,
        COUNT(*) FILTER (WHERE refunded_amount_cents > 0) AS refund_count
      FROM revenue_payments rp WHERE rp.client_id = j.client_id AND rp.job_id = j.id
    ) p ON TRUE
    ${scope.where}
    ORDER BY j.client_id, j.id
  `, scope.values);
  return rows.map(job => {
    const costs = job.actual_direct_cost_cents === null ? job.estimated_direct_cost_cents : job.actual_direct_cost_cents;
    const financials = calculateRevenue({
      quotedAmountCents: number(job.quoted_amount_cents), finalAmountCents: number(job.final_amount_cents),
      jobStatus: job.status, successfulPaymentsCents: number(job.successful_payments_cents),
      refundedCents: number(job.refunded_cents), directCostCents: costs === null ? null : number(costs),
    });
    return { ...job, ...financials, computed_outcome_status: outcomeStatus({ jobStatus: job.status, ...financials, successfulPaymentsCents: number(job.successful_payments_cents) }) };
  });
}

function totalsFromSources(rows) {
  const totals = { ...ZERO_TOTALS };
  for (const row of rows) {
    add(totals, 'booked_revenue_cents', row.bookedRevenueCents);
    add(totals, 'delivered_revenue_cents', row.deliveredRevenueCents);
    add(totals, 'collected_revenue_cents', row.successful_payments_cents);
    add(totals, 'refunded_revenue_cents', row.refundedRevenueCents);
    add(totals, 'net_collected_revenue_cents', row.collectedRevenueCents);
    add(totals, 'payment_count', row.payment_count);
    add(totals, 'refund_count', row.refund_count);
    if ((row.attribution_status || 'unattributed') === 'unattributed') totals.unattributed_outcome_count += 1;
    if (row.attribution_status === 'disputed' || row.computed_outcome_status === 'disputed') totals.disputed_outcome_count += 1;
  }
  totals.job_count = rows.length;
  return totals;
}

async function ledgerSnapshots(db, clientId, from, to) {
  const scope = scopeSql(clientId, from, to, 'e');
  const { rows } = await db.query(`
    SELECT e.client_id, e.event_id, e.event_type, e.occurred_at, e.payload_json
    FROM revenue_events e ${scope.where}
    ORDER BY e.recorded_at, e.event_id
  `, scope.values);
  const snapshots = new Map();
  const unexplained = [];
  for (const event of rows) {
    const outcome = event.payload_json?.outcome || event.payload_json?.result?.revenueOutcome;
    if (outcome?.job_id) snapshots.set(`${event.client_id}:${outcome.job_id}`, outcome);
    else if (event.event_type === 'revenue_outcome_updated') unexplained.push(event.event_id);
  }
  return { eventCount: rows.length, snapshots: [...snapshots.values()], unexplained };
}

async function projectionRows(db, clientId, from, to) {
  const scope = scopeSql(clientId, from, to, 'ro');
  const { rows } = await db.query(`SELECT * FROM revenue_outcomes ro ${scope.where} ORDER BY ro.client_id, ro.job_id`, scope.values);
  return rows;
}

function mismatches(ledger, projection, source) {
  const keys = Object.keys(ZERO_TOTALS);
  return keys.filter(key => ledger[key] !== projection[key] || ledger[key] !== source[key])
    .map(key => ({ metric: key, ledger: ledger[key], projection: projection[key], source: source[key] }));
}

async function applySnapshots(db, snapshots) {
  const columns = [
    'id','client_id','customer_id','prospect_id','company_id','opportunity_id','job_id','payment_id',
    'service_type','lead_source','lead_source_detail','campaign_id','sequence_id','first_touch_agent',
    'last_touch_agent','conversion_agent','human_owner','agent_touch_summary','attribution_status',
    'booked_revenue_cents','delivered_revenue_cents','collected_revenue_cents','refunded_revenue_cents',
    'estimated_direct_cost_cents','actual_direct_cost_cents','gross_profit_cents','gross_margin',
    'sales_cycle_days','time_to_payment_days','outcome_status','occurred_at','payment_count','refund_count',
    'created_at','updated_at',
  ];
  const numeric = new Set(['booked_revenue_cents','delivered_revenue_cents','collected_revenue_cents','refunded_revenue_cents','payment_count','refund_count']);
  const updates = columns.filter(column => !['id','client_id','job_id','created_at'].includes(column))
    .map(column => `${column}=EXCLUDED.${column}`).join(',');
  let count = 0;
  for (const row of snapshots) {
    const values = columns.map(column => numeric.has(column) ? number(row[column]) : row[column] ?? null);
    const placeholders = columns.map((_, index) => `$${index + 1}`).join(',');
    const result = await db.query(`
      INSERT INTO revenue_outcomes (${columns.join(',')}) VALUES (${placeholders})
      ON CONFLICT (client_id,job_id) DO UPDATE SET ${updates}
    `, values);
    count += result.rowCount;
  }
  return count;
}

async function reconcileTenant(db, clientId, options = {}) {
  const ledger = await ledgerSnapshots(db, clientId, options.from, options.to);
  const projected = await projectionRows(db, clientId, options.from, options.to);
  const sources = await sourceRows(db, clientId, options.from, options.to);
  const ledgerTotals = totalsFromOutcomes(ledger.snapshots);
  const projectionTotals = totalsFromOutcomes(projected);
  const sourceTotals = totalsFromSources(sources);
  const differences = mismatches(ledgerTotals, projectionTotals, sourceTotals);
  if (ledger.unexplained.length) differences.push({ metric: 'unexplained_events', event_ids: ledger.unexplained });
  return {
    client_id: number(clientId), ledger_event_count: ledger.eventCount,
    projected_outcome_count: projected.length, source_outcome_count: sources.length,
    unexplained_events: ledger.unexplained, ledger_totals: ledgerTotals,
    projection_totals: projectionTotals, source_totals: sourceTotals,
    mismatches: differences, status: differences.length ? 'failed' : 'passed', snapshots: ledger.snapshots,
  };
}

async function rebuildProjections(db, options = {}) {
  const correlationId = options.correlationId || randomUUID();
  const mode = options.apply ? 'apply' : options.compareOnly ? 'compare_only' : 'dry_run';
  const clients = options.clientId ? [number(options.clientId)] : (await db.query('SELECT id FROM clients ORDER BY id')).rows.map(row => number(row.id));
  const reports = [];
  await db.query('BEGIN');
  try {
    for (const clientId of clients) {
      const flags = await loadRevenueFlags(db, clientId, options.env);
      assertRevenueFlag(flags, 'revenue_schema_enabled');
      if (options.record) assertRevenueFlag(flags, 'revenue_operator_reads_enabled');
    }
    if (mode === 'apply') {
      for (const clientId of clients) {
        const flags = await loadRevenueFlags(db, clientId, options.env);
        assertRevenueFlag(flags, 'revenue_operator_writes_enabled');
      }
    }
    for (const clientId of clients) {
      const before = await reconcileTenant(db, clientId, options);
      if (mode === 'apply') await applySnapshots(db, before.snapshots);
      const after = await reconcileTenant(db, clientId, options);
      const report = { ...after, before_totals: before.projection_totals };
      delete report.snapshots;
      reports.push(report);
      await db.query(`
        INSERT INTO revenue_reconciliation_runs
          (correlation_id,client_id,ledger_totals,projection_totals,source_totals,mismatches,status)
        VALUES ($1,$2,$3::jsonb,$4::jsonb,$5::jsonb,$6::jsonb,$7)
      `, [correlationId, clientId, JSON.stringify(report.ledger_totals), JSON.stringify(report.projection_totals),
        JSON.stringify(report.source_totals), JSON.stringify(report.mismatches), report.status]);
      if (options.record) {
        await db.query(`
          INSERT INTO revenue_operator_audit
            (client_id,actor_type,actor_id,action,source_entity_type,resulting_events,correlation_id,
             source_system,idempotency_key,financial_delta,attribution_confidence)
          VALUES ($1,$2,$3,'revenue_reconciliation','revenue_reconciliation','[]'::jsonb,$4,
                  'revenue_rebuild',$5,$6::jsonb,'unattributed')
          ON CONFLICT (client_id,source_system,idempotency_key) DO NOTHING
        `, [clientId, options.actorType || 'operator', options.actorId || null, correlationId, correlationId,
          JSON.stringify({ net_collected_revenue_cents: report.source_totals.net_collected_revenue_cents })]);
      }
    }
    const summary = {
      execution_correlation_id: correlationId, mode, client_scope: options.clientId ? number(options.clientId) : 'all',
      date_from: options.from || null, date_to: options.to || null,
      ledger_event_count: reports.reduce((n, r) => n + r.ledger_event_count, 0),
      projected_outcome_count: reports.reduce((n, r) => n + r.projected_outcome_count, 0),
      mismatched_records: reports.reduce((n, r) => n + r.mismatches.length, 0),
      unexplained_events: reports.reduce((n, r) => n + r.unexplained_events.length, 0),
      before_totals: reports.map(r => ({ client_id: r.client_id, totals: r.before_totals })),
      after_totals: reports.map(r => ({ client_id: r.client_id, totals: r.projection_totals })),
      tenants: reports, status: reports.every(r => r.status === 'passed') ? 'passed' : 'failed',
    };
    await db.query(`
      INSERT INTO revenue_projection_rebuilds
        (correlation_id,client_id,mode,date_from,date_to,ledger_event_count,projected_outcome_count,
         mismatched_record_count,unexplained_event_count,before_totals,after_totals,status,completed_at)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,$12,NOW())
    `, [correlationId, options.clientId || null, mode, options.from || null, options.to || null,
      summary.ledger_event_count, summary.projected_outcome_count, summary.mismatched_records,
      summary.unexplained_events, JSON.stringify(summary.before_totals), JSON.stringify(summary.after_totals),
      summary.status === 'passed' ? 'passed' : 'failed']);
    if (options.forceRollback) throw Object.assign(new Error('Forced rollback proof'), { code: 'FORCED_ROLLBACK_PROOF' });
    if (mode === 'apply' && summary.status !== 'passed') {
      await db.query('ROLLBACK');
      return { ...summary, applied: false, rolled_back: true };
    }
    await db.query(mode === 'apply' || options.record ? 'COMMIT' : 'ROLLBACK');
    return { ...summary, applied: mode === 'apply', recorded: options.record === true, rolled_back: false };
  } catch (error) {
    await db.query('ROLLBACK').catch(() => {});
    throw error;
  }
}

async function getRevenueHealth(db, clientId) {
  const { rows } = await db.query(`
    SELECT
      (SELECT MAX(recorded_at) FROM revenue_events WHERE client_id=$1) AS latest_event_timestamp,
      (SELECT GREATEST(0, EXTRACT(EPOCH FROM (COALESCE(MAX(e.recorded_at),NOW()) - COALESCE(MAX(ro.updated_at),MAX(e.recorded_at),NOW()))))::bigint
         FROM revenue_events e LEFT JOIN revenue_outcomes ro ON ro.client_id=e.client_id WHERE e.client_id=$1) AS projection_lag_seconds,
      (SELECT COUNT(*)::int FROM revenue_events e WHERE e.client_id=$1 AND e.event_type='revenue_outcome_updated'
         AND NOT EXISTS (SELECT 1 FROM revenue_outcomes ro WHERE ro.client_id=e.client_id AND ro.id=(e.payload_json->>'outcome_id')::uuid)) AS unprojected_event_count,
      (SELECT status FROM revenue_reconciliation_runs WHERE client_id=$1 ORDER BY reconciled_at DESC LIMIT 1) AS reconciliation_status,
      COALESCE((SELECT duplicate_rejection_count FROM revenue_operational_metrics WHERE client_id=$1),0)::bigint AS duplicate_rejection_count,
      COALESCE((SELECT failed_transition_count FROM revenue_operational_metrics WHERE client_id=$1),0)::bigint AS failed_transition_count,
      COALESCE((SELECT tenant_mismatch_count FROM revenue_operational_metrics WHERE client_id=$1),0)::bigint AS tenant_mismatch_count,
      (SELECT completed_at FROM revenue_projection_rebuilds WHERE (client_id=$1 OR client_id IS NULL) AND status='passed' ORDER BY completed_at DESC LIMIT 1) AS last_successful_rebuild
  `, [clientId]);
  return { client_id: number(clientId), ...rows[0] };
}

async function listOperatorAudit(db, clientId, limit = 100) {
  const { rows } = await db.query(`
    SELECT actor_type,actor_id,action,occurred_at,source_entity_type,source_entity_id,
      resulting_events,correlation_id,financial_delta,attribution_confidence
    FROM revenue_operator_audit WHERE client_id=$1 ORDER BY occurred_at DESC LIMIT $2
  `, [clientId, Math.min(Math.max(number(limit), 1), 500)]);
  return rows;
}

module.exports = { ZERO_TOTALS, getRevenueHealth, listOperatorAudit, rebuildProjections, reconcileTenant, totalsFromOutcomes, totalsFromSources };
