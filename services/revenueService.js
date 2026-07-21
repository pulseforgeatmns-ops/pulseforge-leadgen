const { createHash, randomUUID } = require('crypto');
const pool = require('../db');
const {
  assertTransition,
  calculateRevenue,
  domainError,
  normalizeAttributionStatus,
  normalizeLeadSource,
  outcomeStatus,
  requireCents,
} = require('../utils/revenueDomain');

function jsonHash(value) {
  return createHash('sha256').update(JSON.stringify(value)).digest('hex');
}

function asDate(value, field, required = false) {
  if (!value && !required) return null;
  const date = new Date(value);
  if (!value || Number.isNaN(date.getTime())) throw domainError('INVALID_TIMESTAMP', `${field} must be a valid timestamp`);
  return date.toISOString();
}

function mutationContext(input = {}) {
  if (!input.idempotencyKey) throw domainError('IDEMPOTENCY_REQUIRED', 'Idempotency-Key header is required');
  if (String(input.actorType || '').toLowerCase() === 'max') {
    throw domainError('MAX_REVENUE_MUTATION_FORBIDDEN', 'Max revenue access is read-only', 403);
  }
  return {
    sourceSystem: input.sourceSystem || 'pulseforge_manual',
    idempotencyKey: String(input.idempotencyKey),
    sourceEventId: input.sourceEventId || null,
    correlationId: input.correlationId || randomUUID(),
    actorType: input.actorType || 'operator',
    actorId: input.actorId ? String(input.actorId) : null,
    followupRecommendationsEnabled: input.followupRecommendationsEnabled === true,
  };
}

function financialDelta(result = {}) {
  const outcome = result.revenueOutcome || {};
  return {
    booked_revenue_cents: Number(outcome.booked_revenue_cents || 0),
    delivered_revenue_cents: Number(outcome.delivered_revenue_cents || 0),
    collected_revenue_cents: Number(outcome.collected_revenue_cents || 0),
    refunded_revenue_cents: Number(outcome.refunded_revenue_cents || 0),
  };
}

async function writeOperatorAudit(db, clientId, context, result) {
  const events = await db.query(`
    SELECT event_id,event_type,entity_type,entity_id,recorded_at
    FROM revenue_events WHERE client_id=$1 AND correlation_id=$2 ORDER BY recorded_at,event_id
  `, [clientId, context.correlationId]);
  const primary = events.rows.find(row => row.event_type !== 'revenue_outcome_updated') || events.rows[0] || {};
  const outcome = result.revenueOutcome || {};
  await db.query(`
    INSERT INTO revenue_operator_audit
      (client_id,actor_type,actor_id,action,source_entity_type,source_entity_id,resulting_events,
       correlation_id,source_system,idempotency_key,financial_delta,attribution_confidence)
    VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10,$11::jsonb,$12)
    ON CONFLICT (client_id,source_system,idempotency_key) DO NOTHING
  `, [clientId, context.actorType, context.actorId, primary.event_type || 'revenue_mutation',
    primary.entity_type || null, primary.entity_id || null, JSON.stringify(events.rows), context.correlationId,
    context.sourceSystem, context.idempotencyKey, JSON.stringify(financialDelta(result)),
    outcome.attribution_status || 'unattributed']);
}

async function appendEvent(db, { clientId, eventType, entityType, entityId, occurredAt, payload, context, idempotencyKey, isCompensating = false, supersedesEventId = null, causationId = null }) {
  const body = { ...payload };
  const { rows } = await db.query(`
    INSERT INTO revenue_events (
      client_id, event_type, entity_type, entity_id, source_system, source_event_id,
      occurred_at, actor_type, actor_id, correlation_id, causation_id,
      idempotency_key, payload_json, payload_hash, supersedes_event_id, is_compensating_event
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14,$15,$16)
    RETURNING *
  `, [
    clientId, eventType, entityType, entityId, context.sourceSystem,
    idempotencyKey === context.idempotencyKey ? context.sourceEventId : null,
    occurredAt || new Date().toISOString(), context.actorType, context.actorId,
    context.correlationId, causationId, idempotencyKey, JSON.stringify(body), jsonHash(body),
    supersedesEventId, isCompensating,
  ]);
  return rows[0];
}

async function runIdempotent(clientId, rawContext, work) {
  const context = mutationContext(rawContext);
  const db = await pool.connect();
  try {
    await db.query('BEGIN');
    await db.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`${clientId}:${context.sourceSystem}:${context.idempotencyKey}`]);
    const existing = await db.query(`
      SELECT payload_json->'result' AS result
      FROM revenue_events
      WHERE client_id = $1 AND source_system = $2 AND idempotency_key = $3
      LIMIT 1
    `, [clientId, context.sourceSystem, context.idempotencyKey]);
    if (existing.rows[0]) {
      await db.query(`
        INSERT INTO revenue_operational_metrics (client_id,duplicate_rejection_count)
        VALUES ($1,1) ON CONFLICT (client_id) DO UPDATE
        SET duplicate_rejection_count=revenue_operational_metrics.duplicate_rejection_count+1,updated_at=NOW()
      `, [clientId]);
      await db.query('COMMIT');
      return { ...existing.rows[0].result, idempotentReplay: true };
    }
    const result = await work(db, context);
    await writeOperatorAudit(db, clientId, context, result);
    await db.query('COMMIT');
    return { ...result, idempotentReplay: false };
  } catch (error) {
    await db.query('ROLLBACK').catch(() => {});
    if (error.code === '23503') throw domainError('RELATED_ENTITY_NOT_FOUND', 'A related record was not found in this client', 404);
    if (error.code === '23505') throw domainError('DUPLICATE_RECORD', 'This record already exists', 409);
    throw error;
  } finally {
    db.release();
  }
}

async function tenantRow(db, table, clientId, id, { lock = false } = {}) {
  const allowed = new Set(['prospects', 'customers', 'opportunities', 'revenue_jobs', 'revenue_payments']);
  if (!allowed.has(table)) throw new Error('Unsupported tenant table');
  const { rows } = await db.query(
    `SELECT * FROM ${table} WHERE client_id = $1 AND id = $2 LIMIT 1${lock ? ' FOR UPDATE' : ''}`,
    [clientId, id]
  );
  if (!rows[0]) throw domainError('NOT_FOUND', 'Record not found', 404);
  return rows[0];
}

async function createCustomer(clientId, input, rawContext) {
  return runIdempotent(clientId, rawContext, async (db, context) => {
    let prospect = null;
    if (input.prospectId) prospect = await tenantRow(db, 'prospects', clientId, input.prospectId);
    const displayName = String(input.displayName || [prospect?.first_name, prospect?.last_name].filter(Boolean).join(' ') || prospect?.email || '').trim();
    if (!displayName) throw domainError('DISPLAY_NAME_REQUIRED', 'displayName is required');
    const existing = prospect ? await db.query(`SELECT * FROM customers WHERE client_id = $1 AND source_prospect_id = $2 LIMIT 1`, [clientId, prospect.id]) : { rows: [] };
    const { rows } = existing.rows[0] ? existing : await db.query(`
      INSERT INTO customers (
        client_id, customer_type, display_name, primary_email, primary_phone,
        company_id, source_prospect_id, status
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
      RETURNING *
    `, [
      clientId, input.customerType || 'other', displayName,
      input.primaryEmail || prospect?.email || null, input.primaryPhone || prospect?.phone || null,
      input.companyId || prospect?.company_id || null, prospect?.id || null, input.status || 'prospective',
    ]);
    const customer = rows[0];
    const result = { customer };
    await appendEvent(db, {
      clientId, eventType: existing.rows[0] ? 'customer_reused' : prospect ? 'prospect_converted' : 'customer_created',
      entityType: 'customer', entityId: customer.id, occurredAt: customer.created_at,
      payload: { prospect_id: prospect?.id || null, result }, context,
      idempotencyKey: context.idempotencyKey,
    });
    return result;
  });
}

async function createOpportunity(clientId, input, rawContext) {
  return runIdempotent(clientId, rawContext, async (db, context) => {
    const customer = input.customerId ? await tenantRow(db, 'customers', clientId, input.customerId) : null;
    const prospectId = input.prospectId || customer?.source_prospect_id || null;
    if (prospectId) await tenantRow(db, 'prospects', clientId, prospectId);
    if (!customer && !prospectId) throw domainError('OPPORTUNITY_OWNER_REQUIRED', 'customerId or prospectId is required');
    requireCents(input.estimatedValueCents, 'estimatedValueCents');
    const source = normalizeLeadSource(input.source);
    const attributionStatus = normalizeAttributionStatus(input.attributionStatus, Boolean(prospectId));
    const { rows } = await db.query(`
      INSERT INTO opportunities (
        client_id, customer_id, prospect_id, company_id, service_type,
        estimated_value_cents, estimated_cost_cents, expected_close_date, stage,
        source, lead_source_detail, campaign_id, sequence_id, attribution_status, human_owner
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'identified',$9,$10,$11,$12,$13,$14)
      RETURNING *
    `, [
      clientId, customer?.id || null, prospectId, input.companyId || customer?.company_id || null,
      input.serviceType, input.estimatedValueCents, input.estimatedCostCents ?? null,
      input.expectedCloseDate || null, source, input.leadSourceDetail || null,
      input.campaignId || null, input.sequenceId || null, attributionStatus, input.humanOwner || context.actorId,
    ]);
    const opportunity = rows[0];
    const result = { opportunity };
    await appendEvent(db, {
      clientId, eventType: 'opportunity_created', entityType: 'opportunity', entityId: opportunity.id,
      occurredAt: opportunity.created_at, payload: { result }, context, idempotencyKey: context.idempotencyKey,
    });
    return result;
  });
}

async function updateOpportunity(clientId, opportunityId, input, rawContext) {
  return runIdempotent(clientId, rawContext, async (db, context) => {
    const current = await tenantRow(db, 'opportunities', clientId, opportunityId, { lock: true });
    if (!input.stage) throw domainError('STAGE_REQUIRED', 'stage is required');
    assertTransition('opportunity', current.stage, input.stage);
    const terminal = ['won', 'lost', 'cancelled'].includes(input.stage);
    const { rows } = await db.query(`
      UPDATE opportunities
      SET stage = $3, closed_at = CASE WHEN $4 THEN NOW() ELSE closed_at END,
          closed_reason = COALESCE($5, closed_reason), updated_at = NOW()
      WHERE client_id = $1 AND id = $2
      RETURNING *
    `, [clientId, opportunityId, input.stage, terminal, input.closedReason || null]);
    const opportunity = rows[0];
    const result = { opportunity };
    const eventType = input.stage === 'qualified' ? 'opportunity_qualified' : `opportunity_${input.stage}`;
    await appendEvent(db, {
      clientId, eventType, entityType: 'opportunity', entityId: opportunity.id,
      occurredAt: opportunity.updated_at, payload: { transition_from: current.stage, transition_to: input.stage, result },
      context, idempotencyKey: context.idempotencyKey,
    });
    return result;
  });
}

async function createJob(clientId, input, rawContext) {
  return runIdempotent(clientId, rawContext, async (db, context) => {
    const opportunity = await tenantRow(db, 'opportunities', clientId, input.opportunityId, { lock: true });
    if (!['quoted', 'booked', 'won'].includes(opportunity.stage)) {
      throw domainError('OPPORTUNITY_NOT_BOOKABLE', 'Opportunity must be quoted before a job can be created', 409);
    }
    const customerId = input.customerId || opportunity.customer_id;
    await tenantRow(db, 'customers', clientId, customerId);
    const quoted = input.quotedAmountCents ?? Number(opportunity.estimated_value_cents);
    requireCents(quoted, 'quotedAmountCents');
    const scheduledStart = asDate(input.scheduledStart, 'scheduledStart', true);
    const { rows } = await db.query(`
      INSERT INTO revenue_jobs (
        client_id, opportunity_id, customer_id, service_type, service_address,
        scheduled_start, status, assigned_team, quoted_amount_cents,
        estimated_direct_cost_cents
      ) VALUES ($1,$2,$3,$4,$5,$6,'scheduled',$7,$8,$9)
      RETURNING *
    `, [
      clientId, opportunity.id, customerId, input.serviceType || opportunity.service_type,
      input.serviceAddress || null, scheduledStart, input.assignedTeam || null, quoted,
      input.estimatedDirectCostCents ?? opportunity.estimated_cost_cents,
    ]);
    const job = rows[0];
    if (opportunity.stage === 'quoted') {
      await db.query(`UPDATE opportunities SET stage = 'booked', updated_at = NOW() WHERE client_id = $1 AND id = $2`, [clientId, opportunity.id]);
    }
    await projectOutcome(
      db,
      clientId,
      job.id,
      context,
      `${context.idempotencyKey}:projection`,
      null,
      input.scheduledStartPrecision || null
    );
    const result = { job };
    await appendEvent(db, {
      clientId, eventType: 'job_created', entityType: 'job', entityId: job.id,
      occurredAt: job.created_at, context, idempotencyKey: context.idempotencyKey,
      payload: {
        transition_from: null,
        transition_to: 'scheduled',
        temporal_precision: input.scheduledStartPrecision || null,
        result,
      },
    });
    return result;
  });
}

async function startJob(clientId, jobId, input, rawContext) {
  return runIdempotent(clientId, rawContext, async (db, context) => {
    const current = await tenantRow(db, 'revenue_jobs', clientId, jobId, { lock: true });
    assertTransition('job', current.status, 'in_progress');
    const actualStart = asDate(input.actualStart || new Date(), 'actualStart', true);
    const { rows } = await db.query(`
      UPDATE revenue_jobs SET status = 'in_progress', actual_start = $3, updated_at = NOW()
      WHERE client_id = $1 AND id = $2 RETURNING *
    `, [clientId, jobId, actualStart]);
    const result = { job: rows[0] };
    await appendEvent(db, {
      clientId, eventType: 'job_started', entityType: 'job', entityId: jobId,
      occurredAt: actualStart, payload: { transition_from: current.status, transition_to: 'in_progress', result },
      context, idempotencyKey: context.idempotencyKey,
    });
    return result;
  });
}

async function completeJob(clientId, jobId, input, rawContext) {
  return runIdempotent(clientId, rawContext, async (db, context) => {
    const current = await tenantRow(db, 'revenue_jobs', clientId, jobId, { lock: true });
    if (input.completionConfirmed !== true) throw domainError('COMPLETION_CONFIRMATION_REQUIRED', 'completionConfirmed must be true');
    requireCents(input.finalAmountCents, 'finalAmountCents');
    const actualEnd = asDate(input.completionDate, 'completionDate', true);
    if (!current.customer_id || !current.service_type) throw domainError('INCOMPLETE_JOB', 'Job customer and service type are required', 409);
    if (current.status === 'scheduled' || current.status === 'en_route') {
      const validStartFrom = current.status;
      assertTransition('job', validStartFrom, 'in_progress');
      await db.query(`UPDATE revenue_jobs SET status = 'in_progress', actual_start = COALESCE(actual_start, $3), updated_at = NOW() WHERE client_id = $1 AND id = $2`, [clientId, jobId, actualEnd]);
      await appendEvent(db, {
        clientId, eventType: 'job_started', entityType: 'job', entityId: jobId, occurredAt: actualEnd,
        payload: {
          transition_from: validStartFrom,
          transition_to: 'in_progress',
          validated_completion_workflow: true,
          temporal_precision: input.completionDatePrecision || null,
        },
        context, idempotencyKey: `${context.idempotencyKey}:start`,
      });
      current.status = 'in_progress';
    }
    const nextStatus = input.fullyCompleted === false ? 'partially_completed' : 'completed';
    assertTransition('job', current.status, nextStatus);
    const { rows } = await db.query(`
      UPDATE revenue_jobs
      SET status = $3, actual_end = $4, final_amount_cents = $5,
          estimated_direct_cost_cents = COALESCE($6, estimated_direct_cost_cents),
          actual_direct_cost_cents = $7, completion_notes = $8,
          completion_confirmed = TRUE, updated_at = NOW()
      WHERE client_id = $1 AND id = $2 RETURNING *
    `, [
      clientId, jobId, nextStatus, actualEnd, input.finalAmountCents,
      input.estimatedDirectCostCents ?? null, input.actualDirectCostCents ?? null,
      input.completionNotes || null,
    ]);
    const job = rows[0];
    const outcome = await projectOutcome(
      db,
      clientId,
      jobId,
      context,
      `${context.idempotencyKey}:projection`,
      null,
      input.completionDatePrecision || null
    );
    if (context.followupRecommendationsEnabled) await upsertFollowUps(db, clientId, job, input, outcome);
    const result = { job, revenueOutcome: outcome };
    await appendEvent(db, {
      clientId, eventType: nextStatus === 'completed' ? 'job_completed' : 'job_partially_completed',
      entityType: 'job', entityId: jobId, occurredAt: actualEnd,
      payload: {
        transition_from: 'in_progress',
        transition_to: nextStatus,
        temporal_precision: input.completionDatePrecision || null,
        result,
      }, context,
      idempotencyKey: context.idempotencyKey,
    });
    return result;
  });
}

async function recordPayment(clientId, input, rawContext) {
  return runIdempotent(clientId, rawContext, async (db, context) => {
    const job = await tenantRow(db, 'revenue_jobs', clientId, input.jobId, { lock: true });
    requireCents(input.amountCents, 'amountCents', { allowZero: false });
    const status = input.status || 'succeeded';
    if (!['pending', 'succeeded', 'failed'].includes(status)) throw domainError('INVALID_PAYMENT_STATUS', 'Payment status must be pending, succeeded, or failed');
    if (status === 'succeeded') {
      const totals = await db.query(`SELECT COALESCE(SUM(amount_cents - refunded_amount_cents), 0)::bigint AS total FROM revenue_payments WHERE client_id = $1 AND job_id = $2 AND status IN ('succeeded','partially_refunded','refunded')`, [clientId, job.id]);
      const maximum = Number(job.final_amount_cents ?? job.quoted_amount_cents);
      if (!input.allowOverpayment && Number(totals.rows[0].total) + input.amountCents > maximum) {
        throw domainError('PAYMENT_EXCEEDS_JOB_TOTAL', 'Payment exceeds the job total', 409);
      }
    }
    const occurredAt = status === 'succeeded'
      ? asDate(input.receivedAt || new Date(), 'receivedAt', true)
      : status === 'failed' ? asDate(input.failedAt || new Date(), 'failedAt', true) : new Date().toISOString();
    const { rows } = await db.query(`
      INSERT INTO revenue_payments (
        client_id, job_id, customer_id, provider, external_payment_id,
        payment_method, amount_cents, status, received_at, failed_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
      RETURNING *
    `, [
      clientId, job.id, job.customer_id, input.provider || 'manual', input.externalPaymentId || null,
      input.paymentMethod || 'other', input.amountCents, status,
      status === 'succeeded' ? occurredAt : null, status === 'failed' ? occurredAt : null,
    ]);
    const payment = rows[0];
    const outcome = await projectOutcome(
      db,
      clientId,
      job.id,
      context,
      `${context.idempotencyKey}:projection`,
      payment.id,
      input.receivedAtPrecision || null
    );
    const result = { payment, revenueOutcome: outcome };
    await appendEvent(db, {
      clientId, eventType: status === 'succeeded' ? 'payment_succeeded' : status === 'failed' ? 'payment_failed' : 'payment_pending',
      entityType: 'payment', entityId: payment.id, occurredAt,
      payload: {
        invoice_model: 'phase1_job_embedded',
        temporal_precision: input.receivedAtPrecision || null,
        result,
      }, context, idempotencyKey: context.idempotencyKey,
    });
    return result;
  });
}

async function recordRefund(clientId, input, rawContext) {
  return runIdempotent(clientId, rawContext, async (db, context) => {
    const payment = await tenantRow(db, 'revenue_payments', clientId, input.paymentId, { lock: true });
    if (!['succeeded', 'partially_refunded'].includes(payment.status)) throw domainError('PAYMENT_NOT_REFUNDABLE', 'Payment is not refundable', 409);
    requireCents(input.amountCents, 'amountCents', { allowZero: false });
    const refunded = Number(payment.refunded_amount_cents) + input.amountCents;
    if (refunded > Number(payment.amount_cents)) throw domainError('REFUND_EXCEEDS_PAYMENT', 'Refund exceeds successful payment', 409);
    const status = refunded === Number(payment.amount_cents) ? 'refunded' : 'partially_refunded';
    const { rows } = await db.query(`
      UPDATE revenue_payments SET refunded_amount_cents = $3, status = $4, updated_at = NOW()
      WHERE client_id = $1 AND id = $2 RETURNING *
    `, [clientId, payment.id, refunded, status]);
    const updatedPayment = rows[0];
    const outcome = await projectOutcome(db, clientId, payment.job_id, context, `${context.idempotencyKey}:projection`, payment.id);
    const result = { payment: updatedPayment, revenueOutcome: outcome };
    await appendEvent(db, {
      clientId, eventType: 'refund_issued', entityType: 'payment', entityId: payment.id,
      occurredAt: asDate(input.refundedAt || new Date(), 'refundedAt', true),
      payload: { refund_amount_cents: input.amountCents, result }, context,
      idempotencyKey: context.idempotencyKey, isCompensating: true,
    });
    return result;
  });
}

async function touchAttribution(db, clientId, prospectId) {
  if (!prospectId) return {};
  const { rows } = await db.query(`
    SELECT agent, occurred_at FROM (
      SELECT COALESCE(NULLIF(t.channel, ''), 'human') AS agent, t.created_at AS occurred_at
      FROM touchpoints t WHERE t.client_id = $1 AND t.prospect_id = $2
      UNION ALL
      SELECT a.agent_name AS agent, a.ran_at AS occurred_at
      FROM agent_log a WHERE a.client_id = $1 AND a.prospect_id = $2 AND a.status = 'success'
    ) touches WHERE agent IS NOT NULL ORDER BY occurred_at ASC
  `, [clientId, prospectId]);
  if (!rows.length) return {};
  return {
    first_touch: rows[0].agent,
    last_meaningful_touch: rows[rows.length - 1].agent,
    touches: rows.map(row => ({ agent: row.agent, occurred_at: row.occurred_at })),
  };
}

async function projectOutcome(
  db,
  clientId,
  jobId,
  context,
  idempotencyKey,
  paymentId = null,
  temporalPrecision = null
) {
  const { rows } = await db.query(`
    SELECT j.*, o.prospect_id, o.company_id, o.source, o.lead_source_detail,
           o.campaign_id, o.sequence_id, o.attribution_status, o.human_owner,
           o.created_at AS opportunity_created_at
    FROM revenue_jobs j
    LEFT JOIN opportunities o ON o.id = j.opportunity_id AND o.client_id = j.client_id
    WHERE j.client_id = $1 AND j.id = $2
    LIMIT 1
  `, [clientId, jobId]);
  const job = rows[0];
  if (!job) throw domainError('NOT_FOUND', 'Job not found', 404);
  const payments = await db.query(`
    SELECT COALESCE(SUM(amount_cents), 0)::bigint AS succeeded,
           COALESCE(SUM(refunded_amount_cents), 0)::bigint AS refunded,
           MIN(received_at) AS first_received,
           COUNT(*)::int AS payment_count,
           COUNT(*) FILTER (WHERE refunded_amount_cents > 0)::int AS refund_count
    FROM revenue_payments
    WHERE client_id = $1 AND job_id = $2 AND status IN ('succeeded','partially_refunded','refunded')
  `, [clientId, jobId]);
  const payment = payments.rows[0];
  const costs = job.actual_direct_cost_cents === null ? job.estimated_direct_cost_cents : job.actual_direct_cost_cents;
  const financials = calculateRevenue({
    quotedAmountCents: Number(job.quoted_amount_cents),
    finalAmountCents: Number(job.final_amount_cents || 0),
    jobStatus: job.status,
    successfulPaymentsCents: Number(payment.succeeded),
    refundedCents: Number(payment.refunded),
    directCostCents: costs === null ? null : Number(costs),
  });
  const status = outcomeStatus({
    jobStatus: job.status, ...financials, successfulPaymentsCents: Number(payment.succeeded),
  });
  const touches = await touchAttribution(db, clientId, job.prospect_id);
  const occurredAt = job.actual_end || job.scheduled_start;
  const salesCycleDays = job.opportunity_created_at && job.actual_end
    ? (new Date(job.actual_end) - new Date(job.opportunity_created_at)) / 86400000 : null;
  const timeToPaymentDays = job.actual_end && payment.first_received
    ? (new Date(payment.first_received) - new Date(job.actual_end)) / 86400000 : null;
  const { rows: projected } = await db.query(`
    INSERT INTO revenue_outcomes (
      client_id, customer_id, prospect_id, company_id, opportunity_id, job_id, payment_id,
      service_type, lead_source, lead_source_detail, campaign_id, sequence_id,
      first_touch_agent, last_touch_agent, conversion_agent, human_owner, agent_touch_summary,
      attribution_status, booked_revenue_cents, delivered_revenue_cents, collected_revenue_cents,
      refunded_revenue_cents, estimated_direct_cost_cents, actual_direct_cost_cents,
      gross_profit_cents, gross_margin, sales_cycle_days, time_to_payment_days,
      outcome_status, occurred_at, payment_count, refund_count
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17::jsonb,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32)
    ON CONFLICT (client_id, job_id) DO UPDATE SET
      payment_id = COALESCE(EXCLUDED.payment_id, revenue_outcomes.payment_id),
      first_touch_agent = EXCLUDED.first_touch_agent, last_touch_agent = EXCLUDED.last_touch_agent,
      agent_touch_summary = EXCLUDED.agent_touch_summary, booked_revenue_cents = EXCLUDED.booked_revenue_cents,
      delivered_revenue_cents = EXCLUDED.delivered_revenue_cents, collected_revenue_cents = EXCLUDED.collected_revenue_cents,
      refunded_revenue_cents = EXCLUDED.refunded_revenue_cents,
      estimated_direct_cost_cents = EXCLUDED.estimated_direct_cost_cents,
      actual_direct_cost_cents = EXCLUDED.actual_direct_cost_cents,
      gross_profit_cents = EXCLUDED.gross_profit_cents, gross_margin = EXCLUDED.gross_margin,
      sales_cycle_days = EXCLUDED.sales_cycle_days, time_to_payment_days = EXCLUDED.time_to_payment_days,
      outcome_status = EXCLUDED.outcome_status, occurred_at = EXCLUDED.occurred_at,
      payment_count = EXCLUDED.payment_count, refund_count = EXCLUDED.refund_count, updated_at = NOW()
    RETURNING *
  `, [
    clientId, job.customer_id, job.prospect_id, job.company_id, job.opportunity_id, job.id, paymentId,
    job.service_type, job.source || 'unknown', job.lead_source_detail, job.campaign_id, job.sequence_id,
    touches.first_touch || null, touches.last_meaningful_touch || null, context.actorId, job.human_owner,
    JSON.stringify(touches), job.attribution_status || 'unattributed',
    financials.bookedRevenueCents, financials.deliveredRevenueCents, financials.collectedRevenueCents,
    financials.refundedRevenueCents, job.estimated_direct_cost_cents, job.actual_direct_cost_cents,
    financials.grossProfitCents, financials.grossMargin, salesCycleDays, timeToPaymentDays, status, occurredAt,
    Number(payment.payment_count), Number(payment.refund_count),
  ]);
  const outcome = projected[0];
  await appendEvent(db, {
    clientId, eventType: 'revenue_outcome_updated', entityType: 'revenue_outcome', entityId: outcome.id,
    occurredAt: outcome.updated_at,
    payload: {
      outcome_id: outcome.id,
      outcome_status: outcome.outcome_status,
      temporal_precision: temporalPrecision,
      outcome,
    },
    context, idempotencyKey,
  });
  return outcome;
}

async function upsertFollowUps(db, clientId, job, input, outcome) {
  const recurring = /recurring|commercial|turnover|common.area/i.test(job.service_type);
  const entries = [
    ['review', input.requestReview !== false, 'Completed job; approval required before sending', input.reviewAt || null],
    ['referral', false, 'Eligible after successful payment', null],
    ['recurring_service', Boolean(input.recurringEligible ?? recurring), recurring ? 'Service supports recurrence' : 'Operator review required', null],
    ['reactivation', Boolean(input.reactivationDate), input.reactivationDate ? 'Operator selected reactivation date' : 'No reactivation date selected', input.reactivationDate || null],
  ];
  for (const [type, eligible, reason, recommendedAt] of entries) {
    await db.query(`
      INSERT INTO revenue_follow_up_recommendations (client_id, customer_id, job_id, recommendation_type, eligible, recommended_at, reason)
      VALUES ($1,$2,$3,$4,$5,$6,$7)
      ON CONFLICT (client_id, job_id, recommendation_type) DO UPDATE
      SET eligible = EXCLUDED.eligible, recommended_at = EXCLUDED.recommended_at, reason = EXCLUDED.reason, updated_at = NOW()
    `, [clientId, job.customer_id, job.id, type, eligible, recommendedAt, reason]);
  }
  return outcome;
}

async function listRevenueOutcomes(clientId) {
  const { rows } = await pool.query(`
    SELECT ro.*, c.display_name, j.completion_notes,
      COALESCE((SELECT jsonb_agg(jsonb_build_object('type', f.recommendation_type, 'eligible', f.eligible, 'recommended_at', f.recommended_at, 'reason', f.reason, 'status', f.status) ORDER BY f.recommendation_type)
        FROM revenue_follow_up_recommendations f WHERE f.client_id = ro.client_id AND f.job_id = ro.job_id), '[]'::jsonb) AS follow_ups
    FROM revenue_outcomes ro
    JOIN customers c ON c.client_id = ro.client_id AND c.id = ro.customer_id
    JOIN revenue_jobs j ON j.client_id = ro.client_id AND j.id = ro.job_id
    WHERE ro.client_id = $1
    ORDER BY ro.occurred_at DESC
  `, [clientId]);
  return rows;
}

async function getMaxRevenueContext(clientId) {
  const [summary, customers, sources, agents, missing] = await Promise.all([
    pool.query(`SELECT COUNT(*)::int AS outcomes, COALESCE(SUM(booked_revenue_cents),0)::bigint AS booked_revenue_cents, COALESCE(SUM(delivered_revenue_cents),0)::bigint AS delivered_revenue_cents, COALESCE(SUM(collected_revenue_cents),0)::bigint AS collected_revenue_cents, COALESCE(SUM(refunded_revenue_cents),0)::bigint AS refunded_revenue_cents, COALESCE(SUM(gross_profit_cents),0)::bigint AS gross_profit_cents FROM revenue_outcomes WHERE client_id = $1`, [clientId]),
    pool.query(`SELECT c.id AS customer_id, c.display_name, c.status, MAX(ro.occurred_at) AS last_completed_job, COALESCE(SUM(ro.booked_revenue_cents),0)::bigint AS lifetime_booked_revenue_cents, COALESCE(SUM(ro.delivered_revenue_cents),0)::bigint AS lifetime_delivered_revenue_cents, COALESCE(SUM(ro.collected_revenue_cents),0)::bigint AS lifetime_collected_revenue_cents, COALESCE(SUM(ro.gross_profit_cents),0)::bigint AS lifetime_gross_profit_cents, ROUND(AVG(ro.delivered_revenue_cents))::bigint AS average_ticket_cents, COUNT(*) FILTER (WHERE ro.outcome_status = 'paid')::int AS paid_jobs, COUNT(*)::int AS jobs FROM customers c LEFT JOIN revenue_outcomes ro ON ro.client_id = c.client_id AND ro.customer_id = c.id WHERE c.client_id = $1 GROUP BY c.id, c.display_name, c.status ORDER BY lifetime_collected_revenue_cents DESC`, [clientId]),
    pool.query(`SELECT lead_source, COUNT(*)::int AS completed_jobs, COUNT(*) FILTER (WHERE booked_revenue_cents > 0)::int AS bookings, COALESCE(SUM(collected_revenue_cents),0)::bigint AS collected_revenue_cents, COALESCE(SUM(gross_profit_cents),0)::bigint AS gross_profit_cents, ROUND(AVG(sales_cycle_days),2) AS average_sales_cycle_days, ROUND(AVG(time_to_payment_days),2) AS average_time_to_payment_days, ROUND(AVG(CASE WHEN refunded_revenue_cents > 0 THEN 1 ELSE 0 END),4) AS refund_rate FROM revenue_outcomes WHERE client_id = $1 GROUP BY lead_source ORDER BY collected_revenue_cents DESC`, [clientId]),
    pool.query(`SELECT touch->>'agent' AS agent, COUNT(DISTINCT ro.prospect_id)::int AS prospects_touched, COUNT(DISTINCT ro.opportunity_id)::int AS opportunities_influenced, COUNT(DISTINCT ro.job_id)::int AS bookings_influenced, COALESCE(SUM(ro.collected_revenue_cents),0)::bigint AS revenue_influenced_cents, COUNT(*) FILTER (WHERE ro.outcome_status IN ('disputed','refunded','partially_refunded'))::int AS failed_or_disputed_outcomes FROM revenue_outcomes ro CROSS JOIN LATERAL jsonb_array_elements(COALESCE(ro.agent_touch_summary->'touches','[]'::jsonb)) touch WHERE ro.client_id = $1 GROUP BY touch->>'agent' ORDER BY revenue_influenced_cents DESC`, [clientId]),
    pool.query(`SELECT COUNT(*) FILTER (WHERE attribution_status = 'unattributed')::int AS unattributed, COUNT(*) FILTER (WHERE actual_direct_cost_cents IS NULL)::int AS missing_actual_cost, COUNT(*) FILTER (WHERE outcome_status = 'delivered')::int AS completed_unpaid FROM revenue_outcomes WHERE client_id = $1`, [clientId]),
  ]);
  return {
    client_id: Number(clientId),
    generated_at: new Date().toISOString(),
    permissions: { read_only: true, financial_mutations: false, autonomous_follow_up: false },
    summary: summary.rows[0],
    customers: customers.rows,
    sources: sources.rows,
    agents: agents.rows,
    missing_information: missing.rows[0],
  };
}

async function recordOperationalFailure(clientId, code) {
  const column = code === 'INVALID_TRANSITION' ? 'failed_transition_count'
    : ['NOT_FOUND', 'RELATED_ENTITY_NOT_FOUND'].includes(code) ? 'tenant_mismatch_count' : null;
  if (!column) return;
  await pool.query(`
    INSERT INTO revenue_operational_metrics (client_id,${column}) VALUES ($1,1)
    ON CONFLICT (client_id) DO UPDATE SET ${column}=revenue_operational_metrics.${column}+1,updated_at=NOW()
  `, [clientId]);
}

module.exports = {
  completeJob,
  createCustomer,
  createJob,
  createOpportunity,
  getMaxRevenueContext,
  listRevenueOutcomes,
  recordPayment,
  recordRefund,
  recordOperationalFailure,
  startJob,
  updateOpportunity,
};
