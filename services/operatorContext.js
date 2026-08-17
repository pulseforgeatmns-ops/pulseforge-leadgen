'use strict';

/**
 * SPEC-104 — Persistent Operator Context.
 *
 * Max's living operational mental model per client. Rebuilt on meaningful events;
 * session briefs are generated at open time (never stored).
 *
 * Persist → Rebuild → Load → Generate Brief
 */

const crypto = require('crypto');
const { loadApprovedClientIntelligence } = require('../packages/max/workspace/ClientIntelligenceContext');
const objectives = require('./operatorObjectives');

/** @typedef {import('./operatorContextTypes').OperatorContextDocument} OperatorContextDocument */

const REBUILD_TRIGGERS = Object.freeze({
  INTERVIEW_COMPLETED: 'interview_completed',
  BLUEPRINT_APPROVED: 'blueprint_approved',
  CAMPAIGN_LAUNCHED: 'campaign_launched',
  WALKTHROUGH_BOOKED: 'walkthrough_booked',
  JOB_WON: 'job_won',
  CONTENT_PUBLISHED: 'content_published',
  MISSION_COMPLETED: 'mission_completed',
  CLIENT_MESSAGE: 'client_message',
  OUTCOME_RECORDED: 'outcome_recorded',
  PLAYBOOK_UPDATED: 'playbook_updated',
  OBJECTIVE_CHANGED: 'operator_objective_changed',
  MANUAL: 'manual_rebuild',
  SESSION_STALE: 'session_stale',
  INITIAL: 'initial_build',
  OPERATING_EVIDENCE_RECORDED: 'operating_evidence_recorded',
});

const ACTIVE_MISSION_STATUSES = new Set([
  'requested',
  'planning',
  'planned',
  'running',
  'in_progress',
  'active',
  'paused',
  'awaiting_review',
  'review',
]);

const COMPLETED_MISSION_STATUSES = new Set([
  'completed',
  'done',
  'succeeded',
  'failed',
  'cancelled',
]);

class OperatorContextError extends Error {
  constructor(code, message, status = 400) {
    super(message);
    this.name = 'OperatorContextError';
    this.code = code;
    this.status = status;
  }
}

function nowIso() {
  return new Date().toISOString();
}

function newId() {
  return crypto.randomUUID();
}

function asClientId(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.trunc(n);
}

function present(text) {
  return String(text || '')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

function clone(obj) {
  return JSON.parse(JSON.stringify(obj));
}

function emptyContextDocument() {
  return {
    identity: {},
    objectives: [],
    currentPriorities: [],
    activeMissions: [],
    knownRisks: [],
    opportunities: [],
    recentOutcomes: [],
    openQuestions: [],
    sources: {},
  };
}

/* -------------------------------------------------------------------------- */
/* Persistence stores                                                          */
/* -------------------------------------------------------------------------- */

function createMemoryStore() {
  /** @type {Map<string, object>} */
  const rows = new Map();
  /** @type {object[]} */
  const events = [];

  function key(tenantId, clientId) {
    return `${String(tenantId)}:${String(clientId)}`;
  }

  return {
    kind: 'memory',
    async upsert(row) {
      const k = key(row.tenantId, row.clientId);
      rows.set(k, clone(row));
      return clone(row);
    },
    async get(tenantId, clientId) {
      const row = rows.get(key(tenantId, clientId));
      return row ? clone(row) : null;
    },
    async insertRebuildEvent(event) {
      events.push(clone(event));
      return clone(event);
    },
    async listRebuildEvents(filter = {}) {
      let list = [...events];
      if (filter.clientId != null) {
        list = list.filter((e) => Number(e.clientId) === Number(filter.clientId));
      }
      if (filter.tenantId != null) {
        list = list.filter((e) => String(e.tenantId) === String(filter.tenantId));
      }
      list.sort(
        (a, b) => new Date(b.rebuiltAt).getTime() - new Date(a.rebuiltAt).getTime()
      );
      if (filter.limit != null) list = list.slice(0, filter.limit);
      return list.map(clone);
    },
    clear() {
      rows.clear();
      events.length = 0;
    },
  };
}

function mapContextRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    tenantId: row.tenant_id,
    clientId: Number(row.client_id),
    version: Number(row.version),
    context: row.context || emptyContextDocument(),
    lastRebuildTrigger: row.last_rebuild_trigger || null,
    lastRebuildAt:
      row.last_rebuild_at instanceof Date
        ? row.last_rebuild_at.toISOString()
        : String(row.last_rebuild_at),
    createdAt:
      row.created_at instanceof Date
        ? row.created_at.toISOString()
        : String(row.created_at),
    updatedAt:
      row.updated_at instanceof Date
        ? row.updated_at.toISOString()
        : String(row.updated_at),
  };
}

function createPostgresStore(pool) {
  const db = pool || require('../db');
  return {
    kind: 'postgres',
    async upsert(row) {
      const result = await db.query(
        `INSERT INTO operator_contexts (
          id, tenant_id, client_id, version, context,
          last_rebuild_trigger, last_rebuild_at, created_at, updated_at
        ) VALUES (
          COALESCE($1::uuid, gen_random_uuid()), $2, $3, $4, $5::jsonb,
          $6, $7, COALESCE($8::timestamptz, NOW()), NOW()
        )
        ON CONFLICT (tenant_id, client_id) DO UPDATE SET
          version = EXCLUDED.version,
          context = EXCLUDED.context,
          last_rebuild_trigger = EXCLUDED.last_rebuild_trigger,
          last_rebuild_at = EXCLUDED.last_rebuild_at,
          updated_at = NOW()
        RETURNING *`,
        [
          row.id || null,
          String(row.tenantId),
          Number(row.clientId),
          Number(row.version),
          JSON.stringify(row.context || {}),
          row.lastRebuildTrigger || null,
          row.lastRebuildAt || nowIso(),
          row.createdAt || null,
        ]
      );
      return mapContextRow(result.rows[0]);
    },
    async get(tenantId, clientId) {
      const result = await db.query(
        `SELECT * FROM operator_contexts
         WHERE tenant_id = $1 AND client_id = $2
         LIMIT 1`,
        [String(tenantId), Number(clientId)]
      );
      return mapContextRow(result.rows[0]);
    },
    async insertRebuildEvent(event) {
      await db.query(
        `INSERT INTO operator_context_rebuild_events (
          id, tenant_id, client_id, trigger, version,
          context_version_before, rebuilt_at, metadata
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)`,
        [
          event.id || newId(),
          String(event.tenantId),
          Number(event.clientId),
          event.trigger,
          Number(event.version),
          event.contextVersionBefore != null
            ? Number(event.contextVersionBefore)
            : null,
          event.rebuiltAt || nowIso(),
          JSON.stringify(event.metadata || {}),
        ]
      );
      return clone(event);
    },
    async listRebuildEvents(filter = {}) {
      const params = [];
      const clauses = [];
      if (filter.tenantId != null) {
        params.push(String(filter.tenantId));
        clauses.push(`tenant_id = $${params.length}`);
      }
      if (filter.clientId != null) {
        params.push(Number(filter.clientId));
        clauses.push(`client_id = $${params.length}`);
      }
      const where = clauses.length ? `WHERE ${clauses.join(' AND ')}` : '';
      params.push(filter.limit != null ? filter.limit : 50);
      const result = await db.query(
        `SELECT * FROM operator_context_rebuild_events
         ${where}
         ORDER BY rebuilt_at DESC
         LIMIT $${params.length}`,
        params
      );
      return result.rows.map((row) => ({
        id: row.id,
        tenantId: row.tenant_id,
        clientId: Number(row.client_id),
        trigger: row.trigger,
        version: Number(row.version),
        contextVersionBefore:
          row.context_version_before != null
            ? Number(row.context_version_before)
            : null,
        rebuiltAt:
          row.rebuilt_at instanceof Date
            ? row.rebuilt_at.toISOString()
            : String(row.rebuilt_at),
        metadata: row.metadata || {},
      }));
    },
  };
}

function resolveStore(opts = {}) {
  if (opts.store) return opts.store;
  if (opts.pool) return createPostgresStore(opts.pool);
  try {
    const pool = require('../db');
    if (pool && typeof pool.query === 'function') {
      return createPostgresStore(pool);
    }
  } catch (_) {
    /* fall through */
  }
  return createMemoryStore();
}

function defaultObjectiveService() {
  return objectives;
}

function defaultMissionList(opts = {}) {
  if (opts.missionEngine && typeof opts.missionEngine.list === 'function') {
    return (query) => opts.missionEngine.list(query);
  }
  if (opts.pool) {
    return async (query = {}) => {
      const params = [String(query.tenantId)];
      let sql = `SELECT id, tenant_id, client_id, type, status, objective_text, title,
                        created_at, updated_at, completed_at
                 FROM missions WHERE tenant_id = $1`;
      if (query.clientId != null) {
        params.push(Number(query.clientId));
        sql += ` AND client_id = $${params.length}`;
      }
      sql += ' ORDER BY updated_at DESC';
      if (query.limit != null) {
        params.push(Number(query.limit));
        sql += ` LIMIT $${params.length}`;
      }
      const { rows } = await opts.pool.query(sql, params);
      return rows.map((row) => ({
        id: row.id,
        tenantId: row.tenant_id,
        clientId: row.client_id != null ? Number(row.client_id) : null,
        type: row.type,
        status: row.status,
        objectiveText: row.objective_text,
        title: row.title,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
        completedAt: row.completed_at,
      }));
    };
  }
  return async () => [];
}

function defaultOutcomeLoader(opts = {}) {
  if (opts.pool) {
    return async (tenantId, clientId, limit = 8) => {
      try {
        const { rows } = await opts.pool.query(
          `SELECT cbo.outcome_type, cbo.description, cbo.occurred_at,
                  cp.title AS publication_title, cp.channel
           FROM content_business_outcomes cbo
           LEFT JOIN content_publications cp ON cp.id = cbo.publication_id
           WHERE cbo.tenant_id = $1 AND cbo.client_id = $2
           ORDER BY cbo.occurred_at DESC
           LIMIT $3`,
          [String(tenantId), Number(clientId), limit]
        );
        return rows.map((row) => ({
          kind: 'content_outcome',
          summary: present(
            row.description ||
              `${row.outcome_type}${row.publication_title ? `: ${row.publication_title}` : ''}`
          ),
          occurredAt:
            row.occurred_at instanceof Date
              ? row.occurred_at.toISOString()
              : String(row.occurred_at),
          channel: row.channel || null,
        }));
      } catch (_) {
        return [];
      }
    };
  }
  return async () => [];
}

/**
 * Build identity section from approved Blueprint summary.
 *
 * @param {object|null} summary
 * @returns {object}
 */
function buildIdentitySection(summary) {
  if (!summary || !summary.approved) return {};
  return {
    companyName: present(summary.businessName || summary.identity),
    services: present(summary.services),
    serviceArea: present(summary.geography || summary.targetMarkets),
    targetCustomers: present(summary.idealCustomers),
    valueProposition: present(summary.competitiveAdvantages),
    brandVoice: present(summary.brandVoice),
    businessGoals: present(summary.campaignGoals),
  };
}

/**
 * Derive priorities from objectives and blueprint goals.
 *
 * @param {object[]} activeObjectives
 * @param {object|null} summary
 * @returns {string[]}
 */
function deriveCurrentPriorities(activeObjectives, summary) {
  const fromObjectives = (activeObjectives || [])
    .filter((o) => o && o.status === 'active')
    .slice(0, 5)
    .map((o) => present(o.title || o.objectiveText))
    .filter(Boolean);

  if (fromObjectives.length) return fromObjectives.slice(0, 5);

  const goal = summary && present(summary.campaignGoals);
  return goal ? [goal] : [];
}

/**
 * Derive risks and opportunities from blueprint unknowns and playbook constraints.
 *
 * @param {object|null} summary
 * @param {object|null} playbook
 * @param {object[]} missions
 * @returns {{ risks: string[], opportunities: string[] }}
 */
function deriveRisksAndOpportunities(summary, playbook, missions) {
  const risks = [];
  const opportunities = [];

  const unknowns = summary && Array.isArray(summary.unknowns) ? summary.unknowns : [];
  for (const u of unknowns.slice(0, 3)) {
    const text = present(u);
    if (text) risks.push(text);
  }

  if (playbook && Array.isArray(playbook.constraints)) {
    for (const c of playbook.constraints.slice(0, 2)) {
      const text = present(typeof c === 'string' ? c : c && c.text);
      if (text) risks.push(text);
    }
  }

  const activeCount = (missions || []).filter((m) =>
    ACTIVE_MISSION_STATUSES.has(String(m.status || '').toLowerCase())
  ).length;
  if (activeCount === 0 && summary && summary.approved) {
    risks.push('No active missions are running right now.');
  }

  if (playbook && Array.isArray(playbook.preferred_channels)) {
    const channels = playbook.preferred_channels
      .map((c) => present(typeof c === 'string' ? c : c && c.name))
      .filter(Boolean);
    if (channels.length) {
      opportunities.push(`Preferred channels in playbook: ${channels.join(', ')}.`);
    }
  }

  if (summary && present(summary.competitiveAdvantages)) {
    opportunities.push(`Differentiation: ${present(summary.competitiveAdvantages)}.`);
  }

  return {
    risks: risks.slice(0, 5),
    opportunities: opportunities.slice(0, 5),
  };
}

/**
 * Derive open questions from unknowns and thin pipeline signals.
 *
 * @param {object|null} summary
 * @param {object[]} recentOutcomes
 * @returns {string[]}
 */
function deriveOpenQuestions(summary, recentOutcomes) {
  const questions = [];
  const unknowns = summary && Array.isArray(summary.unknowns) ? summary.unknowns : [];
  for (const u of unknowns.slice(0, 4)) {
    const text = present(u);
    if (text && !text.endsWith('?')) {
      questions.push(`What is true about: ${text}?`);
    } else if (text) {
      questions.push(text);
    }
  }
  if (!recentOutcomes.length && summary && summary.approved) {
    questions.push('Which channel is producing the first measurable business outcome?');
  }
  return questions.slice(0, 6);
}

/**
 * Map missions to active mission summaries (no duplication — reference only).
 *
 * @param {object[]} missions
 * @returns {object[]}
 */
function mapActiveMissions(missions) {
  return (missions || [])
    .filter((m) => ACTIVE_MISSION_STATUSES.has(String(m.status || '').toLowerCase()))
    .slice(0, 8)
    .map((m) => ({
      id: m.id,
      title: present(m.title) || present(m.objectiveText) || m.id,
      status: m.status,
      objectiveText: present(m.objectiveText),
      type: m.type || null,
    }));
}

/**
 * Recent completed missions as outcome signals.
 *
 * @param {object[]} missions
 * @returns {object[]}
 */
function mapMissionOutcomes(missions) {
  return (missions || [])
    .filter((m) => COMPLETED_MISSION_STATUSES.has(String(m.status || '').toLowerCase()))
    .slice(0, 5)
    .map((m) => ({
      kind: 'mission',
      summary: `${present(m.title) || 'Mission'} ${m.status}.`,
      occurredAt:
        (m.completedAt && String(m.completedAt)) ||
        (m.updatedAt && String(m.updatedAt)) ||
        nowIso(),
    }));
}

/**
 * Assemble the full operator context document from source systems.
 *
 * @param {object} input
 * @returns {Promise<OperatorContextDocument>}
 */
async function assembleOperatorContext(input = {}) {
  const tenantId = String(input.tenantId || input.clientId || '').trim();
  const clientId = asClientId(input.clientId ?? tenantId);
  if (!tenantId || clientId == null) {
    throw new OperatorContextError('invalid_client', 'tenantId and clientId are required');
  }

  const objectiveService = input.objectiveService || defaultObjectiveService();
  const listMissions = input.listMissions || defaultMissionList(input);
  const loadOutcomes = input.loadOutcomes || defaultOutcomeLoader(input);

  const loaded = await loadApprovedClientIntelligence({
    tenantId,
    clientId,
    cieService: input.cieService,
    cieOpts: input.cieOpts,
  });

  let activeObjectives = [];
  try {
    activeObjectives = await objectiveService.getActiveObjectives(
      { tenantId, clientId, limit: 25 },
      input.objectiveOpts || {}
    );
  } catch (_) {
    activeObjectives = [];
  }

  let missions = [];
  try {
    missions = await listMissions({
      tenantId,
      clientId,
      limit: 25,
    });
  } catch (_) {
    missions = [];
  }

  let contentOutcomes = [];
  try {
    contentOutcomes = await loadOutcomes(tenantId, clientId, 8);
  } catch (_) {
    contentOutcomes = [];
  }

  const summary = loaded.summary;
  const playbook = loaded.playbook;
  const { risks, opportunities } = deriveRisksAndOpportunities(
    summary,
    playbook,
    missions
  );
  const missionOutcomes = mapMissionOutcomes(missions);
  const recentOutcomes = [...contentOutcomes, ...missionOutcomes]
    .sort(
      (a, b) =>
        new Date(b.occurredAt || 0).getTime() - new Date(a.occurredAt || 0).getTime()
    )
    .slice(0, 8);

  const doc = {
    identity: buildIdentitySection(summary),
    objectives: (activeObjectives || []).map((o) => ({
      id: o.id,
      title: present(o.title),
      objectiveText: present(o.objectiveText),
      scope: o.scope,
      status: o.status,
      currentPhase: o.currentPhase || null,
      timeHorizon: o.timeHorizon || null,
    })),
    currentPriorities: deriveCurrentPriorities(activeObjectives, summary),
    activeMissions: mapActiveMissions(missions),
    knownRisks: risks,
    opportunities,
    recentOutcomes,
    openQuestions: deriveOpenQuestions(summary, recentOutcomes),
    sources: {
      blueprintApproved: Boolean(summary && summary.approved),
      blueprintVersion:
        loaded.blueprint && loaded.blueprint.version != null
          ? String(loaded.blueprint.version)
          : null,
      playbookId: playbook && playbook.id ? String(playbook.id) : null,
      playbookVersion: playbook && playbook.version ? String(playbook.version) : null,
      playbookStatus: playbook && playbook.status ? playbook.status : null,
      assembledAt: nowIso(),
    },
  };

  return doc;
}

/**
 * Rebuild and persist operator context for a client.
 *
 * @param {object} input
 * @returns {Promise<object>}
 */
async function rebuildOperatorContext(input = {}) {
  const tenantId = String(input.tenantId || input.clientId || '').trim();
  const clientId = asClientId(input.clientId ?? tenantId);
  const trigger = input.trigger || REBUILD_TRIGGERS.MANUAL;
  const store = resolveStore(input);

  if (!tenantId || clientId == null) {
    throw new OperatorContextError('invalid_client', 'tenantId and clientId are required');
  }

  const existing = await store.get(tenantId, clientId);
  const context = await assembleOperatorContext(input);
  const version = existing ? Number(existing.version) + 1 : 1;
  const rebuiltAt = nowIso();

  const row = await store.upsert({
    id: existing && existing.id ? existing.id : newId(),
    tenantId,
    clientId,
    version,
    context,
    lastRebuildTrigger: trigger,
    lastRebuildAt: rebuiltAt,
    createdAt: existing && existing.createdAt ? existing.createdAt : rebuiltAt,
  });

  await store.insertRebuildEvent({
    id: newId(),
    tenantId,
    clientId,
    trigger,
    version,
    contextVersionBefore: existing ? existing.version : null,
    rebuiltAt,
    metadata: input.metadata || {},
  });

  return row;
}

/**
 * Load persisted operator context; optionally rebuild when missing or stale.
 *
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function loadOperatorContext(input = {}) {
  const tenantId = String(input.tenantId || input.clientId || '').trim();
  const clientId = asClientId(input.clientId ?? tenantId);
  const store = resolveStore(input);

  if (!tenantId || clientId == null) return null;

  let row = await store.get(tenantId, clientId);

  const maxAgeMs =
    input.maxAgeMs != null ? Number(input.maxAgeMs) : 60 * 60 * 1000;
  const stale =
    row &&
    maxAgeMs > 0 &&
    Date.now() - new Date(row.lastRebuildAt).getTime() > maxAgeMs;

  if (!row && input.rebuildIfMissing !== false) {
    row = await rebuildOperatorContext({
      ...input,
      tenantId,
      clientId,
      trigger: REBUILD_TRIGGERS.INITIAL,
    });
  } else if (stale && input.rebuildIfStale) {
    row = await rebuildOperatorContext({
      ...input,
      tenantId,
      clientId,
      trigger: REBUILD_TRIGGERS.SESSION_STALE,
    });
  }

  return row;
}

/**
 * Generate a session brief from operator context — never stored.
 *
 * @param {object} contextRow persisted operator_context row
 * @param {{ hour?: number }} [options]
 * @returns {object}
 */
function generateSessionBrief(contextRow, options = {}) {
  const ctx =
    contextRow && contextRow.context
      ? contextRow.context
      : emptyContextDocument();
  const hour =
    Number.isFinite(options.hour) ? options.hour : new Date().getHours();
  const greeting =
    hour < 12 ? 'Good morning.' : hour < 17 ? 'Good afternoon.' : 'Good evening.';

  const company =
    present(ctx.identity && ctx.identity.companyName) || 'your business';
  const body = [];
  body.push(`I reviewed ${company} before you arrived.`);

  if (ctx.activeMissions && ctx.activeMissions.length) {
    const first = ctx.activeMissions[0];
    body.push(
      `Active mission: ${first.title}${first.status ? ` (${first.status})` : ''}.`
    );
  }

  if (ctx.recentOutcomes && ctx.recentOutcomes.length) {
    const highlights = ctx.recentOutcomes.slice(0, 3).map((o) => o.summary);
    body.push(`Recent outcomes: ${highlights.join('; ')}.`);
  }

  if (ctx.knownRisks && ctx.knownRisks.length) {
    body.push(`Watching: ${ctx.knownRisks[0]}.`);
  }

  if (ctx.objectives && ctx.objectives.length) {
    const obj = ctx.objectives[0];
    body.push(
      `Top objective: ${obj.title}${obj.currentPhase ? ` — ${obj.currentPhase}` : ''}.`
    );
  } else if (ctx.currentPriorities && ctx.currentPriorities.length) {
    body.push(`Current priority: ${ctx.currentPriorities[0]}.`);
  }

  if (ctx.openQuestions && ctx.openQuestions.length) {
    body.push(`Open question I'm tracking: ${ctx.openQuestions[0]}`);
  }

  const recommendations = deriveRecommendedActions(ctx);
  const prompt =
    recommendations.length > 0
      ? `I'd recommend we ${recommendations[0].label.toLowerCase()}.`
      : 'What would you like to investigate?';

  const lines = [greeting, '', ...body, '', prompt];
  return {
    greeting,
    body,
    prompt,
    recommendations,
    reviewedBeforeArrival: true,
    companyName: company,
    contextVersion: contextRow ? contextRow.version : null,
    lastUpdated: contextRow ? contextRow.lastRebuildAt : null,
    fullText: lines.join('\n'),
  };
}

/**
 * Deterministic next-action recommendations from context (generated, not stored).
 *
 * @param {object} ctx
 * @returns {object[]}
 */
function deriveRecommendedActions(ctx) {
  const actions = [];
  if (ctx.activeMissions && ctx.activeMissions.length) {
    actions.push({
      id: 'review_active_mission',
      type: 'review',
      label: `Review active mission "${ctx.activeMissions[0].title}"`,
    });
  }
  if (ctx.currentPriorities && ctx.currentPriorities.length) {
    actions.push({
      id: 'focus_priority',
      type: 'focus',
      label: `Focus on ${ctx.currentPriorities[0]}`,
    });
  }
  if (ctx.recentOutcomes && ctx.recentOutcomes.length) {
    actions.push({
      id: 'review_outcomes',
      type: 'review',
      label: 'Review recent outcomes',
    });
  }
  if (ctx.openQuestions && ctx.openQuestions.length) {
    actions.push({
      id: 'investigate_question',
      type: 'investigate',
      label: 'Investigate an open question',
    });
  }
  if (!actions.length) {
    actions.push({
      id: 'explore_briefing',
      type: 'review',
      label: "Review today's briefing",
    });
  }
  return actions.slice(0, 5);
}

/**
 * Fire-and-forget rebuild hook for meaningful events.
 *
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function triggerOperatorContextRebuild(input = {}) {
  const tenantId = String(input.tenantId || input.clientId || '').trim();
  const clientId = asClientId(input.clientId ?? tenantId);
  if (!tenantId || clientId == null) return null;

  try {
    return await rebuildOperatorContext({
      ...input,
      tenantId,
      clientId,
      trigger: input.trigger || REBUILD_TRIGGERS.MANUAL,
    });
  } catch (err) {
    if (input.logErrors !== false) {
      console.error('[operatorContext] rebuild failed:', err.message);
    }
    return null;
  }
}

/**
 * Build envelope attachment for ContextEnvelope / session context.
 *
 * @param {object|null} row
 * @param {object|null} sessionBrief
 * @returns {object}
 */
function buildOperatorContextAttachment(row, sessionBrief = null) {
  return {
    operatorContext: row
      ? {
          version: row.version,
          lastRebuildAt: row.lastRebuildAt,
          lastRebuildTrigger: row.lastRebuildTrigger,
          document: row.context,
        }
      : null,
    sessionBrief: sessionBrief || null,
    reviewedBeforeArrival: Boolean(sessionBrief && sessionBrief.reviewedBeforeArrival),
  };
}

module.exports = {
  REBUILD_TRIGGERS,
  OperatorContextError,
  emptyContextDocument,
  createMemoryStore,
  createPostgresStore,
  resolveStore,
  assembleOperatorContext,
  rebuildOperatorContext,
  loadOperatorContext,
  generateSessionBrief,
  deriveRecommendedActions,
  triggerOperatorContextRebuild,
  buildOperatorContextAttachment,
};
