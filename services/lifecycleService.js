'use strict';

// Phase A2 canonical prospect lifecycle authority.
//
// Every write that changes a prospect's business stage must flow through
// transitionProspectLifecycle(). The two legacy fields (prospects.status and
// prospects.setter_status) remain stored, but they are updated only through
// the mapping layer below — Pipeline stage moves, setter call dispositions,
// callback scheduling, meeting booking, and dead/do-not-call outcomes all
// converge on the same transactional path and produce one canonical
// prospect_lifecycle_events row.
//
// The legacy mappings are DERIVED FROM PRODUCTION WRITERS, not assumed:
//   - routes/setter.js PATCH /api/leads/:id/status   (stage moves)
//   - utils/callDispositions.js applyProspectDisposition (dispositions)
//   - utils/clientContext.js PROSPECT_STATUSES        (status domain)

const defaultPool = require('../db');
const { ensureLifecycleSchema } = require('../utils/lifecycleSchema');

const CANONICAL_STAGES = Object.freeze(['new', 'contacted', 'follow_up', 'booked', 'dead']);
const CANONICAL_STAGE_SET = new Set(CANONICAL_STAGES);

// Phase B structured lifecycle reasons (additive — the five canonical stages
// are unchanged). A reason qualifies a stage so that outcomes which are NOT
// permanent-dead stop being collapsed into Dead semantics:
//   - nurture:              alive, re-surfaces on a long-dated callback
//   - data_remediation:     alive, needs contact-data repair (e.g. new phone)
//   - terminal_suppression: do-not-call — dead AND globally suppressed (DNC)
const LIFECYCLE_REASONS = Object.freeze(['nurture', 'data_remediation', 'terminal_suppression']);
const LIFECYCLE_REASON_SET = new Set(LIFECYCLE_REASONS);

// Legacy write map. `status: null` = preserve the current legacy status —
// this matches production: setter stage moves never touched prospects.status
// except for `dead`. Overlays from the disposition map may override status.
const LIFECYCLE_LEGACY_MAP = Object.freeze({
  new: { status: null, setter_status: 'new' },
  contacted: { status: null, setter_status: 'contacted' },
  follow_up: { status: null, setter_status: 'follow_up' },
  booked: { status: null, setter_status: 'booked' },
  dead: { status: 'dead', setter_status: 'dead' },
});

// Every production transition is currently reachable from the setter pipeline
// drag/drop, so the matrix is intentionally permissive. Two guards encode
// real business rules that already exist implicitly:
//   - a booked prospect does not silently fall back to `new`
//   - dead requires an explicit reason (enforced by the adapters)
const BLOCKED_TRANSITIONS = new Set(['booked>new']);

// ── Disposition → canonical stage mapping ──────────────────────────────────
// Reviewed inventory of every production disposition (utils/callDispositions.js
// DISPOSITION_VALUES) plus the Phase A2 addition `meeting_booked`. Each entry
// reproduces the exact legacy effects of applyProspectDisposition().
//
// | Disposition              | Canonical stage | Legacy status effect | Extra effects              |
// |--------------------------|-----------------|----------------------|----------------------------|
// | voicemail                | contacted       | preserve             | callback optional          |
// | no_answer                | contacted       | preserve             | callback optional          |
// | gatekeeper_relayed       | follow_up       | preserve             | default next-day callback  |
// | gatekeeper_blocked       | follow_up       | preserve             |                            |
// | answered_callback        | follow_up       | preserve             | callback required (SLA)    |
// | answered_interested      | follow_up       | status='warm'        | is_hot=true                |
// | qualified                | follow_up       | status='warm' (see note) | is_hot=true            |
// | incumbent_all_set        | follow_up       | status='cold'        | is_hot=false, 90d nurture  |
// | answered_not_interested  | follow_up       | status='cold'        | NURTURE reason, 90d cb     |
// | disqualified             | dead            | status='dead'        | callback cleared           |
// | wrong_number             | follow_up       | preserve (phone-only)| phone=NULL, DATA_REMEDIATION|
// | disconnected             | follow_up       | preserve (phone-only)| phone=NULL, DATA_REMEDIATION|
// | do_not_call (Phase B)    | dead            | status='dead'        | do_not_contact=true (DNC)  |
// | meeting_booked           | booked          | preserve             | booked handoff, idempotent |
//
// NOTE on `qualified`: production wrote status='hot', which the startup
// normalizer in utils/clientContext.js immediately rewrites to 'warm'
// ('hot' is outside prospects_status_check). The canonical map writes 'warm'
// directly and sets is_hot=true, which is the observable production result.
//
// Phase B product rules (approved in the Phase B spec — these resolve the
// three Phase A2 DISPOSITIONS_UNDER_REVIEW rows):
//   - answered_not_interested → NURTURE. Alive on follow_up with status cold,
//     hot flag cleared, and a long-dated nurture callback (same pattern as
//     incumbent_all_set). It is no longer collapsed into permanent Dead.
//   - wrong_number / disconnected → DATA REMEDIATION. Phone is cleared and the
//     prospect stays alive on follow_up awaiting phone repair (next action:
//     find_phone). Global status and DNC are untouched.
//   - do_not_call (NEW) → TERMINAL SUPPRESSION. Dead AND do_not_contact=true;
//     the existing suppression trigger cancels pending callbacks and drafts.
const DISPOSITION_STAGE_MAP = Object.freeze({
  voicemail: { stage: 'contacted' },
  no_answer: { stage: 'contacted' },
  gatekeeper_relayed: { stage: 'follow_up' },
  gatekeeper_blocked: { stage: 'follow_up' },
  answered_callback: { stage: 'follow_up' },
  answered_interested: { stage: 'follow_up', statusOverride: 'warm', isHot: true },
  qualified: { stage: 'follow_up', statusOverride: 'warm', isHot: true },
  incumbent_all_set: { stage: 'follow_up', statusOverride: 'cold', isHot: false, lifecycleReason: 'nurture' },
  answered_not_interested: { stage: 'follow_up', statusOverride: 'cold', isHot: false, lifecycleReason: 'nurture' },
  disqualified: { stage: 'dead' },
  wrong_number: { stage: 'follow_up', preserveStatus: true, clearPhone: true, lifecycleReason: 'data_remediation' },
  disconnected: { stage: 'follow_up', preserveStatus: true, clearPhone: true, lifecycleReason: 'data_remediation' },
  do_not_call: { stage: 'dead', suppress: true, lifecycleReason: 'terminal_suppression' },
  meeting_booked: { stage: 'booked' },
});

// Resolved Phase A2 review items — kept as an auditable record of the product
// decision that changed each mapping. Do not re-map without a new product rule.
const DISPOSITIONS_UNDER_REVIEW = Object.freeze({
  answered_not_interested: 'RESOLVED (Phase B): nurture — follow_up + cold + nurture callback, not permanent Dead.',
  wrong_number: 'RESOLVED (Phase B): data_remediation — phone cleared, prospect stays alive for phone repair.',
  disconnected: 'RESOLVED (Phase B): data_remediation — phone cleared, prospect stays alive for phone repair.',
});

function dispositionStageEffects(disposition) {
  const effects = DISPOSITION_STAGE_MAP[String(disposition || '')];
  if (!effects) throw lifecycleError(`No canonical stage mapping for disposition: ${disposition}`, 'UNMAPPED_DISPOSITION');
  return effects;
}

function lifecycleError(message, code = 'LIFECYCLE_INVALID', status = 400) {
  const error = new Error(message);
  error.code = code;
  error.status = status;
  return error;
}

function deriveCanonicalStage(row) {
  const setterStatus = String(row?.setter_status || '').trim().toLowerCase();
  if (CANONICAL_STAGE_SET.has(setterStatus)) return setterStatus;
  if (setterStatus === 'closed') return 'booked';
  const status = String(row?.status || '').trim().toLowerCase();
  if (['dead', 'disqualified'].includes(status)) return 'dead';
  if (status === 'closed') return 'booked';
  if (['contacted', 'warm'].includes(status)) return 'contacted';
  return 'new';
}

function validateTransition(fromStage, toStage) {
  if (!CANONICAL_STAGE_SET.has(toStage)) {
    throw lifecycleError(`Invalid canonical stage: ${toStage}`, 'INVALID_STAGE');
  }
  if (BLOCKED_TRANSITIONS.has(`${fromStage}>${toStage}`)) {
    throw lifecycleError(`Transition ${fromStage} → ${toStage} is not allowed`, 'BLOCKED_TRANSITION', 409);
  }
}

async function opportunitiesTableExists(db) {
  const { rows } = await db.query(`SELECT to_regclass('public.opportunities') AS name`);
  return Boolean(rows[0]?.name);
}

async function findOpenOpportunity(db, clientId, prospectId) {
  if (!(await opportunitiesTableExists(db))) return null;
  const { rows } = await db.query(`
    SELECT id, stage, estimated_value_cents
    FROM opportunities
    WHERE client_id = $1 AND prospect_id = $2
      AND stage NOT IN ('lost', 'cancelled')
    ORDER BY created_at DESC
    LIMIT 1
  `, [clientId, prospectId]);
  return rows[0] || null;
}

async function findPendingHandoffAction(db, clientId, prospectId) {
  const { rows } = await db.query(`
    SELECT id
    FROM agent_actions
    WHERE client_id = $1
      AND action_type = 'closer_handoff'
      AND status = 'pending'
      AND payload->>'prospect_id' = $2::text
    LIMIT 1
  `, [clientId, String(prospectId)]);
  return rows[0] || null;
}

/**
 * Canonical transition service. Runs transactionally: locks the prospect row,
 * validates the transition, updates status + setter_status through the mapping
 * layer, writes one prospect_lifecycle_events row, reconciles callback state,
 * and keeps the booked handoff idempotent.
 *
 * Accepts an external transaction client (`db`) so the call-disposition
 * endpoint can compose it inside its existing transaction; otherwise it
 * manages its own BEGIN/COMMIT.
 *
 * Returns { prospect, event, handoff, opportunity, idempotentReplay }.
 * Email/side channels are NOT touched here — adapters send the closer email
 * after commit using the returned handoff info.
 */
async function transitionProspectLifecycle({
  db = null,
  pool = defaultPool,
  clientId,
  prospectId,
  targetStage,
  reason = null,
  disposition = null,
  dispositionId = null,
  callback,            // undefined = leave callback state untouched
                       // { at: Date|null, mode: 'reschedule'|'complete', completedByDispositionId? }
  statusOverride = null,
  preserveStatus = false,
  clearPhone = false,
  suppress = false,    // terminal suppression: set do_not_contact = true
  lifecycleReason = null, // structured reason code (LIFECYCLE_REASONS) or null
  isHot,               // undefined = leave untouched
  handoffNote = null,
  requireVisible = false,
  requireSource = null,
  actor = {},
  source,
  idempotencyKey = null,
}) {
  if (!clientId || !prospectId) throw lifecycleError('clientId and prospectId are required', 'LIFECYCLE_SCOPE_REQUIRED');
  if (!source) throw lifecycleError('Lifecycle transitions must declare a source', 'LIFECYCLE_SOURCE_REQUIRED');
  if (lifecycleReason && !LIFECYCLE_REASON_SET.has(lifecycleReason)) {
    throw lifecycleError(`Unknown lifecycle reason: ${lifecycleReason}`, 'INVALID_LIFECYCLE_REASON');
  }

  const externalTransaction = Boolean(db);
  const client = db || await pool.connect();
  try {
    await ensureLifecycleSchema(externalTransaction ? client : pool);
    if (!externalTransaction) await client.query('BEGIN');

    if (idempotencyKey) {
      const replay = await client.query(`
        SELECT * FROM prospect_lifecycle_events
        WHERE client_id = $1 AND idempotency_key = $2
        LIMIT 1
      `, [clientId, idempotencyKey]);
      if (replay.rows[0]) {
        const current = await client.query(
          'SELECT * FROM prospects WHERE id = $1 AND client_id = $2',
          [prospectId, clientId]
        );
        if (!externalTransaction) await client.query('COMMIT');
        return {
          prospect: current.rows[0] || null,
          event: replay.rows[0],
          handoff: null,
          opportunity: null,
          idempotentReplay: true,
        };
      }
    }

    const visibilityFilter = requireVisible
      ? `AND COALESCE(p.setter_visible, false) = true
         AND (COALESCE(p.do_not_contact, false) = false OR COALESCE(p.is_synthetic, false) = true)`
      : '';
    const lockParams = [prospectId, clientId];
    let sourceFilter = '';
    if (requireSource) {
      lockParams.push(requireSource);
      sourceFilter = `AND p.source = $${lockParams.length}`;
    }
    const currentResult = await client.query(`
      SELECT p.*, c.name AS company_name
      FROM prospects p
      LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      WHERE p.id = $1 AND p.client_id = $2 ${visibilityFilter} ${sourceFilter}
      FOR UPDATE OF p
    `, lockParams);
    if (!currentResult.rows.length) {
      if (!externalTransaction) await client.query('ROLLBACK');
      throw lifecycleError('Prospect not found', 'PROSPECT_NOT_FOUND', 404);
    }
    const current = currentResult.rows[0];
    const fromStage = deriveCanonicalStage(current);
    validateTransition(fromStage, targetStage);

    const legacy = LIFECYCLE_LEGACY_MAP[targetStage];
    const nextSetterStatus = legacy.setter_status;
    let nextStatus = current.status;
    if (!preserveStatus) {
      if (statusOverride) nextStatus = statusOverride;
      else if (legacy.status) nextStatus = legacy.status;
    }

    const sets = [
      `setter_status = $3`,
      `status = $4`,
      `setter_updated_at = NOW()`,
      `updated_at = NOW()`,
    ];
    const params = [prospectId, clientId, nextSetterStatus, nextStatus];
    if (targetStage === 'booked') {
      sets.push('booked_at = COALESCE(booked_at, NOW())');
    }
    if (clearPhone) sets.push('phone = NULL');
    if (suppress) sets.push('do_not_contact = true');
    if (isHot !== undefined) {
      params.push(Boolean(isHot));
      sets.push(`is_hot = $${params.length}`);
    }
    if (callback !== undefined) {
      params.push(callback?.at ? new Date(callback.at).toISOString() : null);
      sets.push(`callback_at = $${params.length}`);
    }
    if (targetStage === 'dead' && reason) {
      params.push(reason);
      sets.push(`notes = CONCAT(COALESCE(notes, ''), E'\\n\\nDisqualification reason: ', $${params.length}::text)`);
    }

    const updated = await client.query(`
      UPDATE prospects
      SET ${sets.join(', ')}
      WHERE id = $1 AND client_id = $2
      RETURNING *
    `, params);
    const prospect = { ...updated.rows[0], company_name: current.company_name };

    // ── Callback state (dual-store: prospects.callback_at + setter_callbacks)
    if (callback !== undefined) {
      if (callback?.mode === 'complete' && callback.completedByDispositionId) {
        await client.query(`
          UPDATE setter_callbacks
          SET status = 'completed', completed_at = NOW(),
              completed_by_disposition_id = $1, updated_at = NOW()
          WHERE client_id = $2 AND prospect_id = $3 AND status = 'pending'
        `, [callback.completedByDispositionId, clientId, prospectId]);
      } else {
        await client.query(`
          UPDATE setter_callbacks
          SET status = 'cancelled', cancelled_at = NOW(), updated_at = NOW()
          WHERE client_id = $1 AND prospect_id = $2 AND status = 'pending'
        `, [clientId, prospectId]);
      }
      if (callback?.at) {
        await client.query(`
          INSERT INTO setter_callbacks
            (client_id, prospect_id, source_disposition_id, due_at, created_by, is_synthetic)
          VALUES ($1, $2, $3, $4, $5, $6)
        `, [
          clientId, prospectId, dispositionId || callback.completedByDispositionId || null,
          new Date(callback.at).toISOString(),
          Number.isInteger(Number(actor.id)) ? Number(actor.id) : null,
          Boolean(current.is_synthetic),
        ]);
      }
    }

    // ── Booked handoff (idempotent — exactly one pending handoff/opportunity)
    let handoff = null;
    let opportunity = null;
    if (targetStage === 'booked') {
      opportunity = await findOpenOpportunity(client, clientId, prospectId);
      const existingHandoff = await findPendingHandoffAction(client, clientId, prospectId);
      handoff = {
        alreadyBooked: fromStage === 'booked',
        existingHandoffActionId: existingHandoff?.id || null,
        existingOpportunityId: opportunity?.id || null,
      };
    }

    const event = await client.query(`
      INSERT INTO prospect_lifecycle_events
        (client_id, prospect_id, from_stage, to_stage, from_status, to_status,
         from_setter_status, to_setter_status, disposition, disposition_id,
         callback_at, reason, lifecycle_reason, actor_type, actor_id, actor_name,
         source, payload, idempotency_key)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18::jsonb,$19)
      RETURNING *
    `, [
      clientId, prospectId, fromStage, targetStage,
      current.status || null, prospect.status || null,
      current.setter_status || null, prospect.setter_status || null,
      disposition, dispositionId,
      callback?.at ? new Date(callback.at).toISOString() : null,
      reason,
      lifecycleReason,
      actor.type || (actor.role ? 'user' : 'system'),
      actor.id != null ? String(actor.id) : null,
      actor.name || null,
      source,
      JSON.stringify({
        handoff_note: handoffNote || null,
        status_override: statusOverride || null,
        preserve_status: preserveStatus || false,
        clear_phone: clearPhone || false,
        suppress: suppress || false,
        lifecycle_reason: lifecycleReason || null,
      }),
      idempotencyKey,
    ]);

    if (!externalTransaction) await client.query('COMMIT');
    return {
      prospect,
      event: event.rows[0],
      handoff,
      opportunity,
      idempotentReplay: false,
      fromStage,
      toStage: targetStage,
    };
  } catch (err) {
    if (!externalTransaction) {
      try { await client.query('ROLLBACK'); } catch (_rollbackErr) { /* already rolled back */ }
    }
    throw err;
  } finally {
    if (!externalTransaction) client.release();
  }
}

/**
 * Canonical callback write service (Phase A2 §4). One atomic write path for
 * both callback stores: prospects.callback_at (legacy read fallback) and
 * setter_callbacks (canonical). Also records an audit lifecycle event with an
 * unchanged stage so callback history is visible in the canonical stream.
 */
async function scheduleProspectCallback({
  pool = defaultPool,
  clientId,
  prospectId,
  callbackAt,          // Date | null (null clears)
  requireVisible = false,
  requireSource = null,
  actor = {},
  source,
}) {
  if (!source) throw lifecycleError('Callback writes must declare a source', 'LIFECYCLE_SOURCE_REQUIRED');
  const client = await pool.connect();
  try {
    await ensureLifecycleSchema(pool);
    await client.query('BEGIN');
    const visibilityFilter = requireVisible
      ? `AND COALESCE(setter_visible, false) = true
         AND (COALESCE(do_not_contact, false) = false OR COALESCE(is_synthetic, false) = true)`
      : '';
    const params = [prospectId, clientId];
    let sourceFilter = '';
    if (requireSource) {
      params.push(requireSource);
      sourceFilter = `AND source = $${params.length}`;
    }
    const current = await client.query(`
      SELECT * FROM prospects
      WHERE id = $1 AND client_id = $2 ${visibilityFilter} ${sourceFilter}
      FOR UPDATE
    `, params);
    if (!current.rows.length) {
      await client.query('ROLLBACK');
      throw lifecycleError('Prospect not found', 'PROSPECT_NOT_FOUND', 404);
    }
    const row = current.rows[0];
    const at = callbackAt ? new Date(callbackAt).toISOString() : null;

    const updated = await client.query(`
      UPDATE prospects
      SET callback_at = $3, updated_at = NOW()
      WHERE id = $1 AND client_id = $2
      RETURNING *
    `, [prospectId, clientId, at]);
    await client.query(`
      UPDATE setter_callbacks
      SET status = 'cancelled', cancelled_at = NOW(), updated_at = NOW()
      WHERE client_id = $1 AND prospect_id = $2 AND status = 'pending'
    `, [clientId, prospectId]);
    if (at) {
      await client.query(`
        INSERT INTO setter_callbacks (client_id, prospect_id, due_at, created_by, is_synthetic)
        VALUES ($1, $2, $3, $4, $5)
      `, [
        clientId, prospectId, at,
        Number.isInteger(Number(actor.id)) ? Number(actor.id) : null,
        Boolean(row.is_synthetic),
      ]);
    }
    const stage = deriveCanonicalStage(row);
    await client.query(`
      INSERT INTO prospect_lifecycle_events
        (client_id, prospect_id, from_stage, to_stage, from_status, to_status,
         from_setter_status, to_setter_status, callback_at, reason,
         actor_type, actor_id, actor_name, source, payload)
      VALUES ($1,$2,$3,$3,$4,$4,$5,$5,$6,'callback_scheduled',$7,$8,$9,$10,'{}'::jsonb)
    `, [
      clientId, prospectId, stage, row.status || null, row.setter_status || null, at,
      actor.type || 'user', actor.id != null ? String(actor.id) : null, actor.name || null,
      source,
    ]);
    await client.query('COMMIT');
    return { prospect: updated.rows[0], callbackAt: at };
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_rollbackErr) { /* noop */ }
    throw err;
  } finally {
    client.release();
  }
}

module.exports = {
  BLOCKED_TRANSITIONS,
  CANONICAL_STAGES,
  LIFECYCLE_REASONS,
  DISPOSITIONS_UNDER_REVIEW,
  DISPOSITION_STAGE_MAP,
  LIFECYCLE_LEGACY_MAP,
  deriveCanonicalStage,
  dispositionStageEffects,
  scheduleProspectCallback,
  transitionProspectLifecycle,
  validateTransition,
};
