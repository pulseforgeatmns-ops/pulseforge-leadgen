'use strict';

// Phase A2 canonical ProspectWorkspace read model.
//
// One server-side assembly of everything an operator needs to work a
// prospect: identity, normalized phone, canonical lifecycle stage, legacy
// status fields, callback (dual-store precedence), next action, history,
// known facts with sources, operator + legacy notes, call attempt summary,
// opportunity summary, and role-derived permissions. The browser must never
// re-assemble this from unrelated endpoints.
//
// Read-only. Tenant scoping is enforced by every query (client_id = $n) and
// the route layer refuses cross-tenant prospect ids before calling this.

const defaultPool = require('../db');
const { describePhone } = require('../utils/phone');
const { ensureLifecycleSchema } = require('../utils/lifecycleSchema');
const { deriveCanonicalStage } = require('./lifecycleService');
const { callbackSla, dispositionContract } = require('../utils/setterQuality');

const SETTER_NOTES_MARKER = '\n\n--- setter notes ---\n';

const DISPOSITION_LABELS = Object.freeze({
  voicemail: 'Voicemail left',
  no_answer: 'No answer',
  wrong_number: 'Wrong number',
  disconnected: 'Number disconnected',
  gatekeeper_relayed: 'Gatekeeper relayed message',
  gatekeeper_blocked: 'Gatekeeper blocked',
  answered_interested: 'Answered — interested',
  answered_not_interested: 'Answered — not interested',
  answered_callback: 'Answered — callback requested',
  incumbent_all_set: 'All set with current vendor',
  qualified: 'Qualified',
  disqualified: 'Disqualified',
  meeting_booked: 'Meeting booked',
});

const VERTICAL_LABELS = Object.freeze({
  cleaning_company_overflow: 'Cleaning company (overflow)',
  str_manager: 'Short-term rental manager',
  property_manager: 'Property manager',
  realtor: 'Realtor',
  restoration_remodeling_partner: 'Restoration / remodeling partner',
  commercial_office: 'Commercial office',
});

const ANCHOR_PRIORITY = Object.freeze({
  cleaning_company_overflow: 1,
  str_manager: 2,
  property_manager: 3,
  realtor: 4,
  restoration_remodeling_partner: 5,
  commercial_office: 6,
});

function baseNotes(notes) {
  return String(notes || '').split(SETTER_NOTES_MARKER)[0].trim();
}

function setterNotes(notes) {
  const value = String(notes || '');
  const index = value.indexOf(SETTER_NOTES_MARKER);
  return index === -1 ? '' : value.slice(index + SETTER_NOTES_MARKER.length);
}

function verticalLabel(vertical) {
  if (!vertical) return null;
  return VERTICAL_LABELS[vertical] ||
    String(vertical).replaceAll('_', ' ').replace(/^\w/, c => c.toUpperCase());
}

function companyNameFor(row) {
  return row.company_name ||
    baseNotes(row.notes).split('—')[0].trim() ||
    `${row.first_name || ''} ${row.last_name || ''}`.trim() ||
    row.email ||
    'Unknown Lead';
}

function websiteFor(row) {
  return row.company_website || ((baseNotes(row.notes) || '').split('—')[1] || '').trim() || null;
}

function priorityFor(row) {
  const clientId = Number(row.client_id);
  if (clientId === 10) {
    const rank = ANCHOR_PRIORITY[String(row.vertical || '')] || 99;
    return {
      rank,
      label: rank === 99 ? 'Unprioritized' : `Anchor priority ${rank}`,
      reason: rank === 99
        ? 'Anchor category not prioritized'
        : `Anchor priority ${rank}: ${String(row.vertical).replaceAll('_', ' ')}`,
    };
  }
  const score = Number(row.icp_score || 0);
  if (Boolean(row.is_hot)) return { rank: 1, label: 'Hot lead', reason: 'Flagged hot by operator' };
  if (score >= 70) return { rank: 2, label: 'High fit', reason: `ICP score ${score}` };
  if (score >= 40) return { rank: 3, label: 'Medium fit', reason: `ICP score ${score}` };
  return { rank: 4, label: 'Low fit', reason: `ICP score ${score}` };
}

function permissionsFor(user, row) {
  const role = String(user?.role || '');
  const operator = ['admin', 'manager'].includes(role);
  const setterLike = ['setter', 'sales'].includes(role);
  const contactProhibited = Boolean(row.do_not_contact) && !row.is_synthetic;
  return {
    canView: true,
    canCall: (operator || setterLike) && !contactProhibited && Boolean(row.phone),
    canLogCall: operator || setterLike,
    canChangeStage: operator || setterLike,
    canCreateOpportunity: operator || setterLike,
  };
}

function splitLocation(location) {
  const value = String(location || '').trim();
  if (!value) return { city: null, state: null };
  const parts = value.split(',').map(part => part.trim()).filter(Boolean);
  if (parts.length >= 2) return { city: parts[0], state: parts[1] };
  const tokens = value.split(/\s+/);
  if (tokens.length >= 2 && /^[A-Z]{2}$/.test(tokens[tokens.length - 1])) {
    return { city: tokens.slice(0, -1).join(' '), state: tokens[tokens.length - 1] };
  }
  return { city: value, state: null };
}

async function tableExists(db, table) {
  const { rows } = await db.query(`SELECT to_regclass($1) AS name`, [`public.${table}`]);
  return Boolean(rows[0]?.name);
}

// Callback precedence (Phase A2 §4): pending setter_callbacks row (canonical
// store) wins; legacy prospects.callback_at is the fallback. Conflicts are
// surfaced, never silently overwritten.
function resolveCallback(prospectRow, pendingCallback) {
  const legacyAt = prospectRow.callback_at ? new Date(prospectRow.callback_at).toISOString() : null;
  const canonicalAt = pendingCallback?.due_at ? new Date(pendingCallback.due_at).toISOString() : null;
  if (canonicalAt) {
    return {
      dueAt: canonicalAt,
      source: 'setter_callbacks',
      conflict: Boolean(legacyAt && legacyAt !== canonicalAt),
      legacyDueAt: legacyAt,
    };
  }
  if (legacyAt) {
    return { dueAt: legacyAt, source: 'prospects.callback_at', conflict: false, legacyDueAt: legacyAt };
  }
  return { dueAt: null, source: null, conflict: false, legacyDueAt: null };
}

function nextActionFor({ callback, canonicalStage, lastDisposition, lifecycleReason = null }) {
  // Structured lifecycle reasons (Phase B) qualify the stage before the
  // generic branches: data remediation means "repair contact data", not
  // "call again"; nurture with a scheduled callback reads as nurture.
  if (lifecycleReason === 'data_remediation') {
    return { type: 'find_phone', dueAt: null, label: 'Find new phone number', overdue: false };
  }
  if (callback.dueAt) {
    const overdue = new Date(callback.dueAt).getTime() < Date.now();
    const nurture = lifecycleReason === 'nurture';
    return {
      type: nurture ? 'nurture_callback' : 'callback',
      dueAt: callback.dueAt,
      label: overdue
        ? (nurture ? 'Nurture check-in overdue' : 'Callback overdue')
        : (nurture ? 'Nurture check-in scheduled' : 'Callback scheduled'),
      overdue,
    };
  }
  if (canonicalStage === 'booked') {
    return { type: 'closer_handoff', dueAt: null, label: 'Booked — with closer', overdue: false };
  }
  if (canonicalStage === 'dead') {
    const suppressed = lifecycleReason === 'terminal_suppression';
    return {
      type: null,
      dueAt: null,
      label: suppressed ? 'Do not call — suppressed' : 'No further action',
      overdue: false,
    };
  }
  if (lifecycleReason === 'nurture') {
    return { type: 'nurture_callback', dueAt: null, label: 'Nurture — schedule re-check', overdue: false };
  }
  if (lastDisposition) {
    try {
      const contract = dispositionContract(lastDisposition);
      return {
        type: contract.next_action,
        dueAt: null,
        label: `Next: ${String(contract.next_action || '').replaceAll('_', ' ')}`,
        overdue: false,
      };
    } catch (_err) { /* unknown disposition — fall through */ }
  }
  return { type: 'call', dueAt: null, label: 'Make first call', overdue: false };
}

function knownFactsFor(row) {
  const facts = [];
  const push = (id, label, value, sourceType, sourceId = null, observedAt = null) => {
    if (value == null || value === '') return;
    facts.push({
      id, label, value: String(value), sourceType, sourceId,
      verified: true, observedAt: observedAt ? new Date(observedAt).toISOString() : null,
    });
  };
  push('company_name', 'Business name', companyNameFor(row), 'scout', row.company_id, row.created_at);
  push('vertical', 'Category', verticalLabel(row.vertical), 'scout', null, row.created_at);
  push('service_area', 'Service area match', row.service_area_match, 'scout', null, row.created_at);
  const location = splitLocation(row.company_location || row.city);
  push('city', 'City', location.city, 'scout', row.company_id, row.created_at);
  push('website', 'Website', websiteFor(row), 'scout', row.company_id, row.created_at);
  push('icp_score', 'ICP score', row.icp_score != null ? String(row.icp_score) : null, 'scoring', null, row.created_at);
  const contact = `${row.first_name || ''} ${row.last_name || ''}`.trim();
  push('contact', 'Contact', contact || null, 'enrichment', null, row.created_at);
  push('job_title', 'Role', row.job_title, 'enrichment', null, row.created_at);
  push('email', 'Email', row.email, 'enrichment', null, row.created_at);
  if (row.phone) push('phone', 'Phone', describePhone(row.phone).display, 'enrichment', null, row.created_at);
  return facts;
}

async function getProspectWorkspace({
  pool = defaultPool,
  clientId,
  prospectId,
  user = {},
} = {}) {
  await ensureLifecycleSchema(pool);

  const prospectResult = await pool.query(`
    SELECT p.*,
      c.name AS company_name,
      c.location AS company_location,
      c.website AS company_website
    FROM prospects p
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE p.id = $1 AND p.client_id = $2
    LIMIT 1
  `, [prospectId, clientId]);
  if (!prospectResult.rows.length) return null;
  const row = prospectResult.rows[0];

  const hasSetterCallbacks = await tableExists(pool, 'setter_callbacks');
  const hasCallDispositions = await tableExists(pool, 'call_dispositions');
  const hasActivityLog = await tableExists(pool, 'activity_log');

  const [pendingCallbackRes, callSummaryRes, lifecycleEventsRes, notesRes, touchpointsRes, activityRes, lastEventRes] = await Promise.all([
    hasSetterCallbacks
      ? pool.query(`
          SELECT id, due_at, status, created_by, created_at
          FROM setter_callbacks
          WHERE client_id = $1 AND prospect_id = $2 AND status = 'pending'
          ORDER BY created_at DESC
          LIMIT 1
        `, [clientId, prospectId])
      : { rows: [] },
    hasCallDispositions
      ? pool.query(`
          SELECT
            COUNT(*)::int AS disposition_count,
            MAX(created_at) AS last_attempt_at,
            (SELECT disposition FROM call_dispositions
              WHERE client_id = $1 AND prospect_id = $2
              ORDER BY created_at DESC LIMIT 1) AS last_disposition,
            (SELECT COALESCE((details->>'decision_maker_reached')::boolean, NULL)
              FROM call_dispositions
              WHERE client_id = $1 AND prospect_id = $2
              ORDER BY created_at DESC LIMIT 1) AS decision_maker_reached,
            (SELECT details->>'interest_level'
              FROM call_dispositions
              WHERE client_id = $1 AND prospect_id = $2
              ORDER BY created_at DESC LIMIT 1) AS interest_level,
            (SELECT notes FROM call_dispositions
              WHERE client_id = $1 AND prospect_id = $2
              ORDER BY created_at DESC LIMIT 1) AS last_disposition_notes,
            (SELECT structured_notes FROM call_dispositions
              WHERE client_id = $1 AND prospect_id = $2
              ORDER BY created_at DESC LIMIT 1) AS last_structured_notes
          FROM call_dispositions
          WHERE client_id = $1 AND prospect_id = $2
        `, [clientId, prospectId])
      : { rows: [{}] },
    pool.query(`
      SELECT id, from_stage, to_stage, disposition, reason, lifecycle_reason,
        actor_type, actor_name, source, callback_at, created_at
      FROM prospect_lifecycle_events
      WHERE client_id = $1 AND prospect_id = $2
      ORDER BY created_at DESC
      LIMIT 50
    `, [clientId, prospectId]),
    pool.query(`
      SELECT id, note_type, text, author_id, author_name, source, created_at
      FROM prospect_notes
      WHERE client_id = $1 AND prospect_id = $2
      ORDER BY created_at DESC
      LIMIT 100
    `, [clientId, prospectId]),
    pool.query(`
      SELECT t.id::text AS id, t.channel, t.action_type, t.content_summary,
        t.outcome, t.sentiment, t.agent_id, t.created_at, u.name AS actor_name
      FROM touchpoints t
      LEFT JOIN users u ON t.agent_id = u.id::text
      WHERE t.prospect_id = $1 AND t.client_id = $2
      ORDER BY t.created_at DESC
      LIMIT 100
    `, [prospectId, clientId]),
    hasActivityLog
      ? pool.query(`
          SELECT al.id::text AS id, al.action_type, al.notes, al.setter_id, al.created_at
          FROM activity_log al
          WHERE al.lead_id = $1 AND al.client_id = $2
          ORDER BY al.created_at DESC
          LIMIT 100
        `, [prospectId, clientId])
      : { rows: [] },
    pool.query(`
      SELECT to_stage, source, lifecycle_reason, created_at
      FROM prospect_lifecycle_events
      WHERE client_id = $1 AND prospect_id = $2
      ORDER BY created_at DESC
      LIMIT 1
    `, [clientId, prospectId]),
  ]);

  const pendingCallback = pendingCallbackRes.rows[0] || null;
  const callSummary = callSummaryRes.rows[0] || {};
  const callback = resolveCallback(row, pendingCallback);
  const canonicalStage = deriveCanonicalStage(row);

  const legacyCallAttempts = hasActivityLog
    ? Number((await pool.query(`
        SELECT COUNT(*)::int AS count
        FROM activity_log
        WHERE lead_id = $1 AND client_id = $2 AND action_type = 'call'
      `, [prospectId, clientId])).rows[0]?.count || 0)
    : 0;
  const attempts = Number(callSummary.disposition_count || 0) + legacyCallAttempts;

  const history = [
    ...lifecycleEventsRes.rows.map(event => ({
      id: `lifecycle:${event.id}`,
      type: 'lifecycle_transition',
      occurredAt: new Date(event.created_at).toISOString(),
      actorType: event.actor_type,
      actorName: event.actor_name,
      summary: `Stage ${event.from_stage || 'unknown'} → ${event.to_stage}${event.disposition ? ` (${DISPOSITION_LABELS[event.disposition] || event.disposition})` : ''}`,
      details: {
        from_stage: event.from_stage,
        to_stage: event.to_stage,
        disposition: event.disposition,
        reason: event.reason,
        lifecycle_reason: event.lifecycle_reason || null,
        callback_at: event.callback_at,
      },
      source: event.source,
    })),
    ...touchpointsRes.rows.map(touch => ({
      id: `touchpoint:${touch.id}`,
      type: touch.action_type || touch.channel || 'touchpoint',
      occurredAt: new Date(touch.created_at).toISOString(),
      actorType: touch.actor_name ? 'user' : 'agent',
      actorName: touch.actor_name || touch.agent_id || null,
      summary: touch.content_summary || `${touch.channel} ${touch.action_type}`,
      details: touch.outcome ? { outcome: touch.outcome, sentiment: touch.sentiment } : null,
      source: 'touchpoints',
    })),
    ...activityRes.rows.map(activity => ({
      id: `activity:${activity.id}`,
      type: activity.action_type || 'activity',
      occurredAt: new Date(activity.created_at).toISOString(),
      actorType: 'user',
      actorName: activity.setter_id || null,
      summary: activity.notes || activity.action_type,
      details: null,
      source: 'activity_log',
    })),
  ].sort((a, b) => new Date(b.occurredAt) - new Date(a.occurredAt)).slice(0, 100);

  const lastInteraction = history.find(item => item.source !== 'lifecycle_transition') || history[0] || null;
  const lastDisposition = callSummary.last_disposition || null;
  const lifecycleReason = lastEventRes.rows[0]?.lifecycle_reason || null;
  const nextAction = nextActionFor({ callback, canonicalStage, lastDisposition, lifecycleReason });

  // Opportunity summary: the revenue `opportunities` table where present;
  // otherwise the booked handoff (booked_at) is the operational stand-in.
  let opportunity = { exists: false, id: null, stage: null, estimatedValueCents: null };
  if (await tableExists(pool, 'opportunities')) {
    const opportunityRes = await pool.query(`
      SELECT id, stage, estimated_value_cents
      FROM opportunities
      WHERE client_id = $1 AND prospect_id = $2
        AND stage NOT IN ('lost', 'cancelled')
      ORDER BY created_at DESC
      LIMIT 1
    `, [clientId, prospectId]);
    if (opportunityRes.rows[0]) {
      opportunity = {
        exists: true,
        id: opportunityRes.rows[0].id,
        stage: opportunityRes.rows[0].stage,
        estimatedValueCents: opportunityRes.rows[0].estimated_value_cents != null
          ? Number(opportunityRes.rows[0].estimated_value_cents)
          : null,
      };
    }
  }
  if (!opportunity.exists && canonicalStage === 'booked' && row.booked_at) {
    opportunity = { exists: true, id: null, stage: 'booked_handoff', estimatedValueCents: row.mrr_value != null ? Math.round(Number(row.mrr_value) * 100) : null };
  }

  const location = splitLocation(row.company_location || row.city);
  const lastEvent = lastEventRes.rows[0] || null;
  const legacyScratchpad = setterNotes(row.notes);
  const legacyBase = baseNotes(row.notes);

  return {
    prospect: {
      id: row.id,
      clientId: Number(row.client_id),
      companyId: row.company_id || null,
      companyName: companyNameFor(row),
      contactName: `${row.first_name || ''} ${row.last_name || ''}`.trim() || null,
      contactRole: row.job_title || null,
      phone: describePhone(row.phone),
      email: row.email || null,
      website: websiteFor(row),
      city: location.city,
      state: location.state,
      timezone: null,
      vertical: row.vertical || null,
      verticalLabel: verticalLabel(row.vertical),
      source: row.source || null,
      score: row.icp_score != null ? Number(row.icp_score) : null,
      priority: priorityFor(row),
      isHot: Boolean(row.is_hot),
      contactProhibited: Boolean(row.do_not_contact || row.is_synthetic),
      isSynthetic: Boolean(row.is_synthetic),
    },
    lifecycle: {
      canonicalStage,
      lifecycleReason,
      legacyStatus: row.status || null,
      setterStatus: row.setter_status || null,
      lastTransitionAt: lastEvent ? new Date(lastEvent.created_at).toISOString() : null,
      lastTransitionSource: lastEvent?.source || null,
    },
    nextAction,
    callback: {
      dueAt: callback.dueAt,
      source: callback.source,
      conflict: callback.conflict,
      legacyDueAt: callback.legacyDueAt,
      sla: callbackSla(callback.dueAt),
    },
    lastInteraction: lastInteraction
      ? { occurredAt: lastInteraction.occurredAt, summary: lastInteraction.summary, type: lastInteraction.type }
      : null,
    history,
    knownFacts: knownFactsFor(row),
    notes: {
      operatorNotes: notesRes.rows.map(note => ({
        id: note.id,
        noteType: note.note_type,
        text: note.text,
        createdAt: new Date(note.created_at).toISOString(),
        author: note.author_name || (note.author_id != null ? String(note.author_id) : null),
        source: note.source,
      })),
      legacyNotes: legacyScratchpad || null,
      legacyBaseNotes: legacyBase || null,
      summary: null,
    },
    calling: {
      attempts,
      lastAttemptAt: callSummary.last_attempt_at ? new Date(callSummary.last_attempt_at).toISOString() : null,
      lastDisposition,
      lastDispositionLabel: lastDisposition ? (DISPOSITION_LABELS[lastDisposition] || lastDisposition) : null,
      lastDispositionNotes: callSummary.last_disposition_notes || null,
      lastStructuredNotes: (() => {
        const raw = callSummary.last_structured_notes;
        if (!raw) return null;
        if (typeof raw === 'object') return raw;
        try { return JSON.parse(raw); } catch { return null; }
      })(),
      decisionMakerReached: callSummary.decision_maker_reached ?? null,
      interestLevel: callSummary.interest_level || null,
    },
    opportunity,
    permissions: permissionsFor(user, row),
    generatedAt: new Date().toISOString(),
  };
}

async function addProspectNote({
  pool = defaultPool,
  clientId,
  prospectId,
  noteType = 'operator',
  text,
  author = {},
  source = 'workspace',
} = {}) {
  await ensureLifecycleSchema(pool);
  const clean = String(text || '').trim().slice(0, 5000);
  if (!clean) {
    const err = new Error('Note text is required');
    err.status = 400;
    throw err;
  }
  const { rows } = await pool.query(`
    INSERT INTO prospect_notes (client_id, prospect_id, note_type, text, author_id, author_name, source)
    SELECT p.client_id, p.id, $3, $4, $5, $6, $7
    FROM prospects p
    WHERE p.id = $1 AND p.client_id = $2
    RETURNING *
  `, [
    prospectId, clientId, noteType, clean,
    Number.isInteger(Number(author.id)) ? Number(author.id) : null,
    author.name || null, source,
  ]);
  if (!rows.length) {
    const err = new Error('Prospect not found');
    err.status = 404;
    throw err;
  }
  return rows[0];
}

module.exports = {
  DISPOSITION_LABELS,
  SETTER_NOTES_MARKER,
  addProspectNote,
  getProspectWorkspace,
  resolveCallback,
  verticalLabel,
};
