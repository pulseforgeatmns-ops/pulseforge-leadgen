'use strict';

/**
 * P0 #1087 — Read-only operational inspection.
 *
 * An operator asking for an operational status report ("Give me a read only
 * operational status report for anchor cleaning…") is asking Max to READ the
 * tenant's persisted operating state. It is not mission planning, mission
 * continuation, an approval, or execution. Before this route existed, the
 * request fell through identity → reasoning fallback → Client Intelligence
 * "unknowns" mode, which rendered a Blueprint-only "Mission Updated —
 * Acquisition Mission Planning / Approve discovery?" card (no persisted
 * mutation, but fabricated mission progress presented as state).
 *
 * Contract:
 *  - Tenant comes ONLY from the authenticated workspace session
 *    (session.context.tenantId, verified by routes/maxWorkspace.js). A client
 *    named in the question that is not the session tenant is refused — no
 *    data is read for it.
 *  - Every database read goes through a guarded read-only queryable that
 *    rejects any statement that is not a plain SELECT/WITH read. Mission
 *    runtimes, approvals, execution routers, and Blueprint content are never
 *    consulted or called.
 *  - Each source reports provenance, as-of timestamp, and availability.
 *    Unavailable sources are disclosed; they never become "healthy".
 *  - Mission classification criteria are documented below and in
 *    docs/P0-MAX-OPERATIONAL-INSPECTION.md. Uncertain → labelled uncertain.
 */

const { buildStructuredResponse } = require('./WorkspaceTypes');
const { stripNegatedDirectives } = require('./NegatedDirective');
const askPathTrace = require('./audit/AskPathTrace');

const SPEC = 'P0-1087-OPERATIONAL-INSPECTION';

/** A non-terminal mission with no recorded activity for this long is stale. */
const STALE_AFTER_DAYS = 14;
const DAY_MS = 24 * 60 * 60 * 1000;

const CATEGORIES = Object.freeze({
  ACTIVE: 'active',
  COMPLETED: 'completed',
  STALE: 'stale',
  DEVELOPMENT_TEST: 'development_test',
  UNCERTAIN: 'uncertain',
});

const CATEGORY_LABELS = Object.freeze({
  [CATEGORIES.ACTIVE]: 'Active business missions',
  [CATEGORIES.STALE]: 'Stale missions',
  [CATEGORIES.COMPLETED]: 'Completed / closed missions',
  [CATEGORIES.DEVELOPMENT_TEST]: 'Development / test missions',
  [CATEGORIES.UNCERTAIN]: 'Uncertain classification',
});

const COMPLETED_STATUSES = new Set(['complete', 'completed', 'reviewed', 'done']);
const CLOSED_STATUSES = new Set(['cancelled', 'canceled', 'archived', 'abandoned', 'superseded', 'retired']);
const TEST_NAME_RE = /\b(test|testing|demo|fixture|sandbox|dev|qa|probe|canary|dummy|smoke)\b/i;

// ---------------------------------------------------------------------------
// Intent detection
// ---------------------------------------------------------------------------

const OPERATIONAL_CUE =
  /\boperational\s+(status|state|health|report|overview|snapshot|picture|view|inspection|summary|review)\b|\boperations\s+(status|report|overview|snapshot|summary|review)\b/i;
const REPORT_CUE =
  /\b(status|state|health|mission|portfolio)\s+(report|overview|snapshot|summary|inventory|audit)\b|\b(report|inventory|audit|inspection|overview)\s+(on|of)\s+(all\s+)?(the\s+)?(my\s+)?(missions|operations)\b/i;
const READ_ONLY_CUE =
  /\bread[\s-]?only\b|\bwithout\s+(changing|modifying|touching|archiving|deleting|executing|sending)\b|\b(do\s+not|don['’]?t|dont)\s+(archive|delete|modify|change|touch|execute|send|approve)\b/i;
const PORTFOLIO_CUE =
  /\bmissions\b|\b(active|completed|stale|test|development|dev)\b[^.?!]{0,60}\bmissions?\b|\bexecution\s+blockers\b|\boperations\b|\boutbound\s+activity\b/i;
// After negated clauses are removed, any remaining command verb means the turn
// asks for an action and must keep its normal routing.
const ACTION_COMMAND =
  /\b(approve|execute|launch|archive|delete|cancel|pause|resume|retry|proceed|send\s+(the|it|them|outreach|emails?)|run\s+(scout|discovery|outreach|emmett|the\s+mission)|start\s+(the\s+)?(mission|discovery|outreach)|create\s+(a\s+)?(new\s+)?mission|continue\s+(the\s+)?mission)\b/i;

/**
 * @param {string} question
 * @returns {{ matched: boolean, reason: string, cues: string[] }}
 */
function detectOperationalInspection(question) {
  const raw = String(question || '').trim();
  if (!raw) return { matched: false, reason: 'empty', cues: [] };
  const affirmative = stripNegatedDirectives(raw);
  if (ACTION_COMMAND.test(affirmative)) {
    return { matched: false, reason: 'affirmative_action_command', cues: [] };
  }
  const cues = [];
  if (OPERATIONAL_CUE.test(raw)) cues.push('operational_report');
  if (REPORT_CUE.test(raw)) cues.push('status_report');
  if (READ_ONLY_CUE.test(raw)) cues.push('read_only');
  if (PORTFOLIO_CUE.test(raw)) cues.push('portfolio_scope');
  const has = (c) => cues.includes(c);
  const asksForState = /\b(status|state|report|blockers?|overview|snapshot|health)\b/i.test(raw);

  if (has('operational_report')) return { matched: true, reason: 'operational_report_request', cues };
  if (has('status_report') && (has('read_only') || has('portfolio_scope'))) {
    return { matched: true, reason: 'scoped_status_report_request', cues };
  }
  if (has('read_only') && has('portfolio_scope') && asksForState) {
    return { matched: true, reason: 'read_only_portfolio_status_request', cues };
  }
  return { matched: false, reason: 'no_operational_inspection_cue', cues };
}

function isOperationalInspectionRequest(question) {
  return detectOperationalInspection(question).matched;
}

/**
 * P0 #1089 — true when the affirmative (negation-stripped) text still carries
 * an action command. Shared with the analytical follow-up contract so both
 * operational owners decline the same commands.
 */
function hasAffirmativeActionCommand(question) {
  return ACTION_COMMAND.test(stripNegatedDirectives(String(question || '')));
}

// ---------------------------------------------------------------------------
// Read-only data access
// ---------------------------------------------------------------------------

const WRITE_SQL_RE =
  /\b(insert|update|delete|upsert|merge|alter|create|drop|truncate|grant|revoke|copy|vacuum|reindex|cluster|comment|lock|call|do|nextval|setval|pg_advisory_lock|pg_advisory_xact_lock)\b/i;

/**
 * Wrap a pg-like pool so only plain read statements can reach it.
 * @param {{ query: Function }} pool
 * @param {{ writesBlocked: number }} boundary
 */
function readOnlyQueryable(pool, boundary) {
  return {
    async query(sql, params) {
      const text = typeof sql === 'string' ? sql : String((sql && sql.text) || '');
      const isRead = /^\s*(select|with)\b/i.test(text);
      if (!isRead || WRITE_SQL_RE.test(text) || /\bfor\s+(update|share|no\s+key\s+update|key\s+share)\b/i.test(text)) {
        boundary.writesBlocked += 1;
        const err = new Error('operational_inspection_read_only_violation');
        err.code = 'OPERATIONAL_INSPECTION_READ_ONLY';
        throw err;
      }
      boundary.reads += 1;
      return pool.query(sql, params);
    },
  };
}

function resolvePool(opts = {}) {
  if (opts.pool && typeof opts.pool.query === 'function') return opts.pool;
  if (opts.pool === null) return null;
  if (!process.env.DATABASE_URL) return null;
  try {
    return require('../../../db');
  } catch (_) {
    return null;
  }
}

function iso(value) {
  if (value == null || value === '') return null;
  const d = value instanceof Date ? value : new Date(value);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function maxIso(...values) {
  const list = values.map(iso).filter(Boolean).sort();
  return list.length ? list[list.length - 1] : null;
}

function unavailable(source, reason) {
  return { source, available: false, reason, rows: [] };
}

async function loadAcquisitionMissions({ db, tenantId, runtime, asOf }) {
  const source = 'acquisition_missions';
  if (db) {
    try {
      const { missionFromRow } = require('../../../services/acquisitionMissionPersistence');
      const missions = (await db.query(
        `SELECT * FROM acquisition_missions WHERE tenant_id = $1 ORDER BY updated_at DESC LIMIT 100`,
        [String(tenantId)]
      )).rows.map(missionFromRow);
      let lastEvents = new Map();
      try {
        const events = await db.query(
          `SELECT DISTINCT ON (mission_id) mission_id, kind, label, at
             FROM acquisition_mission_events
            WHERE tenant_id = $1
            ORDER BY mission_id, at DESC`,
          [String(tenantId)]
        );
        lastEvents = new Map((events.rows || []).map((r) => [r.mission_id, { kind: r.kind, label: r.label, at: iso(r.at) }]));
      } catch (_) {
        lastEvents = null;
      }
      return {
        source,
        available: true,
        provenance: 'Postgres acquisition_missions (+ acquisition_mission_events)',
        asOf,
        eventsAvailable: lastEvents !== null,
        rows: missions.map((m) => normalizeAmoMission(m, lastEvents ? lastEvents.get(m.id) : undefined)),
      };
    } catch (err) {
      if (/relation .* does not exist/i.test(String(err && err.message))) {
        return { source, available: true, provenance: 'Postgres acquisition_missions (table absent — no durable missions)', asOf, rows: [] };
      }
      return unavailable(source, err && err.code === 'OPERATIONAL_INSPECTION_READ_ONLY' ? 'read_only_violation' : 'acquisition_mission_store_unavailable');
    }
  }
  if (runtime && typeof runtime.engine === 'function') {
    try {
      const engine = runtime.engine();
      const store = engine && engine.store;
      const missions = typeof engine.list === 'function' ? engine.list(String(tenantId)) : [];
      return {
        source,
        available: true,
        provenance: 'in-process acquisition mission runtime (non-durable view — no database configured)',
        asOf,
        durable: false,
        eventsAvailable: Boolean(store && typeof store.listEvents === 'function'),
        rows: missions
          .filter((m) => String(m.tenantId || m.clientId || '') === String(tenantId))
          .map((m) => {
            const events = store && typeof store.listEvents === 'function' ? store.listEvents(m.id) : [];
            const last = events.length ? events[events.length - 1] : null;
            return normalizeAmoMission(m, last ? { kind: last.kind, label: last.label, at: iso(last.at) } : undefined);
          }),
      };
    } catch (_) {
      return unavailable(source, 'acquisition_mission_runtime_unavailable');
    }
  }
  return unavailable(source, 'acquisition_mission_store_not_configured');
}

function normalizeAmoMission(m, lastEvent) {
  const blockers = [];
  for (const b of Array.isArray(m.blockers) ? m.blockers : []) {
    const label = b && (b.reason || b.label || b.code) ? String(b.reason || b.label || b.code) : String(b);
    if (label) blockers.push({ kind: 'mission_blocker', label });
  }
  if (m.pendingOperatorDecision && typeof m.pendingOperatorDecision === 'object') {
    blockers.push({
      kind: 'awaiting_operator_decision',
      label: `Awaiting operator decision: ${m.pendingOperatorDecision.prompt || m.pendingOperatorDecision.kind || 'decision'} (${m.pendingOperatorDecision.kind || 'unspecified'})`,
    });
  }
  const synthetic = Boolean(m.syntheticEvidence && m.syntheticEvidence.provenance === 'synthetic_fixture');
  if (synthetic) {
    blockers.push({ kind: 'synthetic_evidence', label: 'Outreach refused: mission carries synthetic (fixture) Scout evidence' });
  }
  const constraints = m.authorizationConstraints && Array.isArray(m.authorizationConstraints.prohibited)
    ? m.authorizationConstraints.prohibited
    : [];
  if (constraints.length) {
    blockers.push({ kind: 'operator_restriction', label: `Operator restrictions in force: ${constraints.join(', ')}` });
  }
  return {
    system: 'acquisition_mission',
    id: m.id,
    title: m.title || null,
    objective: m.objective || null,
    stage: m.stage || null,
    status: m.status || null,
    planCancelled: m.planCancelled === true,
    createdAt: iso(m.createdAt),
    updatedAt: iso(m.updatedAt),
    lastEvent: lastEvent || null,
    synthetic,
    testFlag: m.test === true || /^(test|development|dev)$/i.test(String(m.environment || '')),
    blockers,
  };
}

async function loadEngineMissions({ db, tenantId, asOf }) {
  const source = 'missions';
  if (!db) return unavailable(source, 'mission_store_not_configured');
  try {
    const { rows } = await db.query(
      `SELECT id, tenant_id, client_id, type, status, objective_text, title,
              created_at, updated_at, completed_at
         FROM missions
        WHERE tenant_id = $1
        ORDER BY updated_at DESC
        LIMIT 100`,
      [String(tenantId)]
    );
    return {
      source,
      available: true,
      provenance: 'Postgres missions (SPEC-022 mission engine)',
      asOf,
      rows: (rows || []).map((r) => ({
        system: 'mission_engine',
        id: r.id,
        title: r.title || null,
        objective: r.objective_text || null,
        stage: null,
        status: r.status || null,
        planCancelled: false,
        createdAt: iso(r.created_at),
        updatedAt: iso(r.updated_at),
        completedAt: iso(r.completed_at),
        lastEvent: null,
        synthetic: false,
        testFlag: false,
        blockers: r.status === 'failed' ? [{ kind: 'mission_failed', label: 'Mission engine status: failed' }] : [],
      })),
    };
  } catch (err) {
    if (/relation .* does not exist/i.test(String(err && err.message))) {
      return { source, available: true, provenance: 'Postgres missions (table absent — no SPEC-022 missions)', asOf, rows: [] };
    }
    return unavailable(source, 'mission_store_unavailable');
  }
}

async function loadProspecting({ db, clientId, asOf }) {
  const source = 'prospects';
  if (!db) return unavailable(source, 'prospect_store_not_configured');
  try {
    const { rows } = await db.query(
      `SELECT COUNT(*)::int AS total,
              COUNT(*) FILTER (WHERE COALESCE(icp_score, 0) >= 70)::int AS qualified,
              COUNT(*) FILTER (WHERE COALESCE(setter_visible, false) = true)::int AS setter_visible,
              COUNT(*) FILTER (WHERE COALESCE(do_not_contact, false) = true)::int AS do_not_contact,
              MAX(created_at) AS last_added_at,
              MAX(last_contacted_at) AS last_contacted_at
         FROM prospects
        WHERE client_id = $1`,
      [clientId]
    );
    const r = rows[0] || {};
    return {
      source,
      available: true,
      provenance: 'Postgres prospects (aggregate counts only)',
      asOf,
      counts: {
        total: Number(r.total) || 0,
        qualified: Number(r.qualified) || 0,
        setterVisible: Number(r.setter_visible) || 0,
        doNotContact: Number(r.do_not_contact) || 0,
      },
      lastAddedAt: iso(r.last_added_at),
      lastContactedAt: iso(r.last_contacted_at),
    };
  } catch (_) {
    return unavailable(source, 'prospect_store_unavailable');
  }
}

async function loadOutboundActivity({ db, clientId, asOf }) {
  const source = 'touchpoints';
  if (!db) return unavailable(source, 'touchpoint_store_not_configured');
  try {
    const { rows } = await db.query(
      `SELECT COALESCE(NULLIF(channel, ''), 'unspecified') AS channel,
              COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '7 days')::int AS last_7d,
              COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '30 days')::int AS last_30d,
              MAX(created_at) AS last_at
         FROM touchpoints
        WHERE client_id = $1
        GROUP BY 1
        ORDER BY last_at DESC NULLS LAST
        LIMIT 12`,
      [clientId]
    );
    return {
      source,
      available: true,
      provenance: 'Postgres touchpoints (aggregate by channel)',
      asOf,
      channels: (rows || []).map((r) => ({
        channel: String(r.channel),
        last7d: Number(r.last_7d) || 0,
        last30d: Number(r.last_30d) || 0,
        lastAt: iso(r.last_at),
      })),
    };
  } catch (_) {
    return unavailable(source, 'touchpoint_store_unavailable');
  }
}

async function loadAgentRuns({ db, clientId, asOf }) {
  const source = 'agent_log';
  if (!db) return unavailable(source, 'agent_log_not_configured');
  try {
    const { rows } = await db.query(
      `SELECT agent_name,
              MAX(ran_at) AS last_run_at,
              COUNT(*) FILTER (WHERE ran_at > NOW() - INTERVAL '7 days')::int AS runs_7d,
              COUNT(*) FILTER (WHERE ran_at > NOW() - INTERVAL '7 days' AND status IN ('error', 'failed'))::int AS errors_7d
         FROM agent_log
        WHERE client_id = $1
        GROUP BY agent_name
        ORDER BY last_run_at DESC NULLS LAST
        LIMIT 20`,
      [clientId]
    );
    return {
      source,
      available: true,
      provenance: 'Postgres agent_log (last run per agent)',
      asOf,
      agents: (rows || []).map((r) => ({
        agent: String(r.agent_name),
        lastRunAt: iso(r.last_run_at),
        runs7d: Number(r.runs_7d) || 0,
        errors7d: Number(r.errors_7d) || 0,
      })),
    };
  } catch (_) {
    return unavailable(source, 'agent_log_unavailable');
  }
}

const ANCHOR_TENANT_ID = '10';

async function loadOutboundGate({ db, tenantId, asOf, env }) {
  const source = 'outbound_gate';
  if (String(tenantId) !== ANCHOR_TENANT_ID) {
    return { source, available: true, applicable: false, provenance: 'no tenant-specific outbound recovery gate', asOf };
  }
  if (!db) return unavailable(source, 'outbound_gate_store_not_configured');
  try {
    const { loadRecoveryState } = require('../../../services/anchorEmailRecovery');
    const state = await loadRecoveryState(db, { env: env || process.env, lockControl: false });
    return {
      source,
      available: true,
      applicable: true,
      provenance: 'Anchor email recovery control (services/anchorEmailRecovery.loadRecoveryState)',
      asOf,
      mode: state && state.mode ? String(state.mode) : 'unknown',
      reason: state && state.reason ? String(state.reason) : null,
    };
  } catch (_) {
    return unavailable(source, 'outbound_gate_unavailable');
  }
}

async function loadClientDirectory({ db }) {
  if (!db) return { available: false, rows: [] };
  try {
    const { rows } = await db.query('SELECT id, name, slug FROM clients ORDER BY id LIMIT 500');
    return { available: true, rows: rows || [] };
  } catch (_) {
    return { available: false, rows: [] };
  }
}

async function safe(fn, fallback) {
  try {
    const out = await fn();
    return out == null ? fallback : out;
  } catch (_) {
    return fallback;
  }
}

// ---------------------------------------------------------------------------
// Tenant authorization for a client named in the question
// ---------------------------------------------------------------------------

function normalizeName(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

/**
 * The tenant is always the session tenant. If the operator names a DIFFERENT
 * known client, refuse rather than silently answering about another tenant.
 */
function resolveNamedClientScope(question, tenantId, directory) {
  if (!directory || !directory.available) return { ok: true, check: 'unavailable', named: null };
  const q = ` ${normalizeName(question)} `;
  const mentions = (directory.rows || []).filter((row) => {
    const names = [row.name, row.slug].map(normalizeName).filter((n) => n.length >= 4);
    return names.some((n) => q.includes(` ${n} `));
  });
  const self = (directory.rows || []).find((r) => String(r.id) === String(tenantId)) || null;
  const mentionsSelf = mentions.some((r) => String(r.id) === String(tenantId));
  const other = mentions.find((r) => String(r.id) !== String(tenantId));
  if (other && !mentionsSelf) {
    return { ok: false, check: 'mismatch', named: { id: String(other.id), name: other.name }, self };
  }
  return { ok: true, check: mentionsSelf ? 'matched' : 'not_named', named: null, self };
}

/**
 * P0 #1089 — named-client authorization for a turn that reuses evidence
 * already in the session. Reads only the client directory (read-only guard).
 */
async function checkNamedClientScope({ question, tenantId, opts = {} } = {}) {
  const boundary = { enforced: true, reads: 0, writesBlocked: 0 };
  const rawPool = resolvePool(opts);
  const db = rawPool ? readOnlyQueryable(rawPool, boundary) : null;
  const loaders = opts.loaders || {};
  const directory = await safe(
    () => (loaders.loadClientDirectory || loadClientDirectory)({ db, tenantId: String(tenantId) }),
    { available: false, rows: [] }
  );
  return { scope: resolveNamedClientScope(question, String(tenantId), directory), mutationBoundary: boundary };
}

// ---------------------------------------------------------------------------
// Classification (documented criteria)
// ---------------------------------------------------------------------------

/**
 * Criteria (in precedence order):
 *  1. development_test — grounded only by persisted markers: synthetic Scout
 *     evidence (syntheticEvidence.provenance = synthetic_fixture) or an
 *     explicit test/development flag. Names alone never decide this
 *     (docs/roadmap/MAX-MISSION-LIFECYCLE-HYGIENE.md).
 *  2. completed — persisted status complete/completed/reviewed/done, or a
 *     closed status (cancelled/archived/…) or planCancelled.
 *  3. uncertain — non-terminal mission whose title/objective looks like test
 *     residue but has no persisted test marker, or with no activity timestamp.
 *  4. stale — non-terminal, last recorded activity older than STALE_AFTER_DAYS.
 *  5. active — non-terminal, recorded activity within STALE_AFTER_DAYS.
 */
function classifyMission(mission, now = new Date()) {
  const status = String(mission.status || '').toLowerCase();
  const lastActivityAt = maxIso(mission.updatedAt, mission.lastEvent && mission.lastEvent.at, mission.completedAt);
  const ageDays = lastActivityAt ? (now.getTime() - new Date(lastActivityAt).getTime()) / DAY_MS : null;
  const base = { lastActivityAt, ageDays: ageDays == null ? null : Math.max(0, Math.floor(ageDays)) };

  if (mission.synthetic) {
    return { ...base, category: CATEGORIES.DEVELOPMENT_TEST, certainty: 'grounded', basis: 'persisted synthetic (fixture) Scout evidence marker' };
  }
  if (mission.testFlag) {
    return { ...base, category: CATEGORIES.DEVELOPMENT_TEST, certainty: 'grounded', basis: 'persisted test/development flag' };
  }
  if (COMPLETED_STATUSES.has(status)) {
    return { ...base, category: CATEGORIES.COMPLETED, certainty: 'grounded', basis: `persisted status "${mission.status}"` };
  }
  if (CLOSED_STATUSES.has(status) || mission.planCancelled) {
    return { ...base, category: CATEGORIES.COMPLETED, certainty: 'grounded', basis: `closed — persisted status "${mission.status || 'cancelled'}"` };
  }
  const nameText = `${mission.title || ''} ${mission.objective || ''}`;
  if (TEST_NAME_RE.test(nameText)) {
    return { ...base, category: CATEGORIES.UNCERTAIN, certainty: 'uncertain', basis: 'title/objective resembles test residue, but no persisted test marker — not classified as test from name alone' };
  }
  if (!lastActivityAt) {
    return { ...base, category: CATEGORIES.UNCERTAIN, certainty: 'uncertain', basis: 'no persisted activity timestamp' };
  }
  if (ageDays > STALE_AFTER_DAYS) {
    return { ...base, category: CATEGORIES.STALE, certainty: 'grounded', basis: `non-terminal, no recorded activity for ${Math.floor(ageDays)} days (> ${STALE_AFTER_DAYS})` };
  }
  return { ...base, category: CATEGORIES.ACTIVE, certainty: 'grounded', basis: `non-terminal, activity within ${STALE_AFTER_DAYS} days` };
}

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

/**
 * @param {object} input
 * @param {string} input.tenantId — authenticated session tenant
 * @param {object} [input.opts] — { pool, now, env, loaders: { … } } (tests inject)
 * @param {object} [input.acquisitionMissionRuntime] — used only when no DB is configured
 */
async function loadOperationalSnapshot(input = {}) {
  const opts = input.opts || {};
  const now = opts.now instanceof Date ? opts.now : new Date();
  const asOf = now.toISOString();
  const tenantId = String(input.tenantId);
  const clientId = Number(tenantId);
  const boundary = { enforced: true, reads: 0, writesBlocked: 0 };
  const rawPool = resolvePool(opts);
  const db = rawPool ? readOnlyQueryable(rawPool, boundary) : null;
  const loaders = opts.loaders || {};
  const ctx = { db, tenantId, clientId, asOf, env: opts.env, runtime: input.acquisitionMissionRuntime || null };

  // Authorization first: a different named client is refused before any
  // tenant operating data is read.
  const directory = await safe(() => (loaders.loadClientDirectory || loadClientDirectory)(ctx), { available: false, rows: [] });
  const scope = input.question != null
    ? resolveNamedClientScope(input.question, tenantId, directory)
    : { ok: true, check: 'not_requested', named: null, self: null };
  if (!scope.ok) {
    return { spec: SPEC, tenantId, asOf, refused: 'tenant_mismatch', scope, directory, sources: [], missions: [], mutationBoundary: boundary };
  }

  const [amoMissions, engineMissions, prospecting, outbound, agentRuns, outboundGate] = await Promise.all([
    safe(() => (loaders.loadAcquisitionMissions || loadAcquisitionMissions)(ctx), unavailable('acquisition_missions', 'loader_failed')),
    safe(() => (loaders.loadEngineMissions || loadEngineMissions)(ctx), unavailable('missions', 'loader_failed')),
    safe(() => (loaders.loadProspecting || loadProspecting)(ctx), unavailable('prospects', 'loader_failed')),
    safe(() => (loaders.loadOutboundActivity || loadOutboundActivity)(ctx), unavailable('touchpoints', 'loader_failed')),
    safe(() => (loaders.loadAgentRuns || loadAgentRuns)(ctx), unavailable('agent_log', 'loader_failed')),
    safe(() => (loaders.loadOutboundGate || loadOutboundGate)(ctx), unavailable('outbound_gate', 'loader_failed')),
  ]);

  const missions = [
    ...(amoMissions.available ? amoMissions.rows : []),
    ...(engineMissions.available ? engineMissions.rows : []),
  ]
    .filter((m) => m && m.id)
    .map((m) => ({ ...m, classification: classifyMission(m, now) }));

  const sources = [amoMissions, engineMissions, prospecting, outbound, agentRuns, outboundGate].map((s) => ({
    source: s.source,
    available: s.available === true,
    provenance: s.provenance || null,
    asOf: s.available ? s.asOf || asOf : null,
    reason: s.available ? null : s.reason || 'unavailable',
  }));

  return {
    spec: SPEC,
    tenantId,
    asOf,
    staleAfterDays: STALE_AFTER_DAYS,
    missions,
    prospecting,
    outbound,
    agentRuns,
    outboundGate,
    directory,
    scope,
    sources,
    mutationBoundary: boundary,
  };
}

function tenantBlockers(snapshot) {
  const out = [];
  const gate = snapshot.outboundGate;
  if (gate && gate.available && gate.applicable && gate.mode && gate.mode !== 'active' && gate.mode !== 'legacy') {
    out.push({
      kind: 'outbound_gate',
      label: `Outbound email gate is in ${gate.mode}${gate.reason ? ` (${gate.reason})` : ''} — sends are held`,
      source: 'outbound_gate',
    });
  }
  const agents = snapshot.agentRuns && snapshot.agentRuns.available ? snapshot.agentRuns.agents : [];
  for (const a of agents) {
    if (a.errors7d > 0) {
      out.push({ kind: 'agent_errors', label: `${a.agent}: ${a.errors7d} error run(s) in the last 7 days`, source: 'agent_log' });
    }
  }
  return out;
}

// ---------------------------------------------------------------------------
// Presentation
// ---------------------------------------------------------------------------

function fmt(ts) {
  return ts ? ts.replace('T', ' ').replace(/\.\d+Z$/, 'Z') : 'not recorded';
}

function missionLine(m) {
  const c = m.classification;
  const stage = [m.stage, m.status].filter(Boolean).join(' / ') || 'status not recorded';
  const last = m.lastEvent && m.lastEvent.at
    ? `last event ${fmt(m.lastEvent.at)} (${m.lastEvent.kind || m.lastEvent.label || 'event'})`
    : `last updated ${fmt(m.updatedAt)}`;
  const parts = [`- ${m.title || m.id} [${m.id}] — ${stage}; ${last}; ${c.basis}`];
  for (const b of m.blockers || []) parts.push(`    • ${b.label}`);
  return parts.join('\n');
}

function composeOperationalReport(snapshot, ctx = {}) {
  const tenantLabel = ctx.tenantName ? `${ctx.tenantName} (tenant ${snapshot.tenantId})` : `tenant ${snapshot.tenantId}`;
  const lines = [];
  lines.push(`Operational status — ${tenantLabel}`);
  lines.push(`Read-only inspection as of ${fmt(snapshot.asOf)}. Nothing was created, approved, executed, archived, deleted, or modified.`);

  const missionSourcesAvailable = snapshot.sources.filter((s) => (s.source === 'acquisition_missions' || s.source === 'missions') && s.available);
  lines.push('');
  lines.push('Mission state');
  if (!missionSourcesAvailable.length) {
    lines.push('- Mission state unavailable: no mission store could be read. This is not evidence of healthy or idle execution.');
  } else if (!snapshot.missions.length) {
    lines.push('- No persisted missions recorded for this tenant in the readable mission stores.');
  } else {
    const order = [CATEGORIES.ACTIVE, CATEGORIES.STALE, CATEGORIES.UNCERTAIN, CATEGORIES.COMPLETED, CATEGORIES.DEVELOPMENT_TEST];
    for (const cat of order) {
      const rows = snapshot.missions.filter((m) => m.classification.category === cat);
      if (!rows.length) continue;
      lines.push(`${CATEGORY_LABELS[cat]} (${rows.length})`);
      for (const m of rows) lines.push(missionLine(m));
    }
  }

  lines.push('');
  lines.push('Active execution blockers');
  const missionBlockers = snapshot.missions
    .filter((m) => m.classification.category === CATEGORIES.ACTIVE || m.classification.category === CATEGORIES.STALE)
    .flatMap((m) => (m.blockers || []).map((b) => `- ${m.title || m.id}: ${b.label}`));
  const tBlockers = tenantBlockers(snapshot).map((b) => `- ${b.label} [${b.source}]`);
  const allBlockers = [...tBlockers, ...missionBlockers];
  if (allBlockers.length) lines.push(...allBlockers);
  else lines.push('- No blockers recorded in the readable sources. Unavailable sources are listed below and were not assumed clear.');

  lines.push('');
  lines.push('Prospecting & outbound activity');
  const p = snapshot.prospecting;
  if (p && p.available) {
    lines.push(`- Prospects: ${p.counts.total} total, ${p.counts.qualified} at ICP ≥ 70, ${p.counts.setterVisible} setter-visible, ${p.counts.doNotContact} do-not-contact; last added ${fmt(p.lastAddedAt)}; last contacted ${fmt(p.lastContactedAt)}.`);
  } else {
    lines.push(`- Prospects: unavailable (${(p && p.reason) || 'unavailable'}).`);
  }
  const o = snapshot.outbound;
  if (o && o.available) {
    if (!o.channels.length) lines.push('- Touchpoints: none recorded.');
    for (const ch of o.channels) lines.push(`- Touchpoints / ${ch.channel}: ${ch.last7d} in 7d, ${ch.last30d} in 30d; last ${fmt(ch.lastAt)}.`);
  } else {
    lines.push(`- Touchpoints: unavailable (${(o && o.reason) || 'unavailable'}).`);
  }
  const a = snapshot.agentRuns;
  if (a && a.available) {
    if (!a.agents.length) lines.push('- Agent runs: none recorded.');
    for (const ag of a.agents.slice(0, 8)) lines.push(`- Agent ${ag.agent}: last run ${fmt(ag.lastRunAt)}; ${ag.runs7d} run(s) in 7d${ag.errors7d ? `, ${ag.errors7d} error(s)` : ''}.`);
  } else {
    lines.push(`- Agent runs: unavailable (${(a && a.reason) || 'unavailable'}).`);
  }
  const g = snapshot.outboundGate;
  if (g && g.available && g.applicable) lines.push(`- Outbound gate: ${g.mode}${g.reason ? ` (${g.reason})` : ''}.`);

  lines.push('');
  lines.push('Evidence sources');
  for (const s of snapshot.sources) {
    lines.push(s.available
      ? `- ✓ ${s.source}: ${s.provenance || 'read'} — as of ${fmt(s.asOf)}`
      : `- ✗ ${s.source}: UNAVAILABLE (${s.reason})`);
  }
  lines.push('');
  lines.push(`Classification criteria: development/test only from persisted markers (synthetic evidence or explicit flag), never from names; completed = persisted completed/closed status; stale = non-terminal with no recorded activity for more than ${snapshot.staleAfterDays} days; active = non-terminal with activity inside that window; anything else is labelled uncertain. Approved Blueprint content is not used as execution evidence.`);
  return lines.join('\n');
}

function countByCategory(missions) {
  const out = {};
  for (const cat of Object.values(CATEGORIES)) out[cat] = 0;
  for (const m of missions) out[m.classification.category] += 1;
  return out;
}

function refusalTurn({ scope, tenantId }) {
  const prose = [
    'Operational status — not inspected',
    `You named ${scope.named.name}, but this workspace is signed in to ${scope.self && scope.self.name ? scope.self.name : `tenant ${tenantId}`}.`,
    'I did not read another tenant\'s data. Switch the active client to that tenant and ask again.',
  ].join('\n');
  return {
    prose,
    structured: buildStructuredResponse({
      answer: prose,
      reasoning: ['P0 #1087 — tenant comes from the authenticated session; a different named client is refused.'],
      confidence: 1,
      metadata: {
        operationalInspection: true,
        operationalInspectionRefused: 'tenant_mismatch',
        businessIntelligenceUsed: false,
      },
    }),
    reason: 'operational_inspection_tenant_mismatch',
    snapshot: null,
  };
}

/**
 * @returns {Promise<null|{ prose, structured, reason, snapshot, detection }>}
 */
async function maybeHandleOperationalInspectionTurn(input = {}) {
  const { question, session } = input;
  const detection = detectOperationalInspection(question);
  askPathTrace.traceBranch('operational_inspection_detection', { matched: detection.matched, reason: detection.reason, cues: detection.cues });
  if (!detection.matched) return null;

  const tenantId = session && session.context && session.context.tenantId;
  if (tenantId == null || tenantId === '') {
    const prose = 'Operational status — not inspected\nNo active client is selected for this workspace session, so there is no tenant to inspect.';
    return {
      prose,
      structured: buildStructuredResponse({ answer: prose, confidence: 1, metadata: { operationalInspection: true, operationalInspectionRefused: 'no_tenant' } }),
      reason: 'operational_inspection_no_tenant',
      snapshot: null,
      detection,
    };
  }

  const snapshot = await loadOperationalSnapshot({
    tenantId,
    question,
    opts: input.opts || {},
    acquisitionMissionRuntime: input.acquisitionMissionRuntime || null,
  });

  const scope = snapshot.scope;
  if (!scope.ok) {
    logInspection({ tenantId, detection, snapshot, refused: 'tenant_mismatch' });
    return { ...refusalTurn({ scope, tenantId }), detection };
  }

  const tenantName = (scope.self && scope.self.name) || null;
  const prose = composeOperationalReport(snapshot, { tenantName });
  const counts = countByCategory(snapshot.missions);
  const unavailableSources = snapshot.sources.filter((s) => !s.available).map((s) => `${s.source}: ${s.reason}`);
  const structured = buildStructuredResponse({
    answer: prose,
    reasoning: [
      'P0 #1087 — read-only operational inspection over persisted tenant state; no mission runtime, approval, or execution path was invoked.',
    ],
    supportingEvidence: snapshot.sources
      .filter((s) => s.available)
      .map((s) => ({ id: `operational:${s.source}`, kind: 'operational_source', label: s.provenance || s.source, source: s.source, timestamp: s.asOf })),
    confidence: unavailableSources.length ? 0.6 : 0.9,
    recommendedActions: [],
    metadata: {
      operationalInspection: true,
      businessIntelligenceUsed: false,
      asOf: snapshot.asOf,
      unavailable: unavailableSources,
      operationalSnapshot: {
        tenantId: snapshot.tenantId,
        asOf: snapshot.asOf,
        staleAfterDays: snapshot.staleAfterDays,
        missionCounts: counts,
        missions: snapshot.missions.map((m) => ({
          id: m.id,
          system: m.system,
          title: m.title,
          stage: m.stage,
          status: m.status,
          updatedAt: m.updatedAt,
          lastEvent: m.lastEvent,
          category: m.classification.category,
          certainty: m.classification.certainty,
          basis: m.classification.basis,
          lastActivityAt: m.classification.lastActivityAt,
          blockers: m.blockers,
        })),
        tenantBlockers: tenantBlockers(snapshot),
        sources: snapshot.sources,
        mutationBoundary: snapshot.mutationBoundary,
        namedClientCheck: scope.check,
      },
    },
  });
  logInspection({ tenantId, detection, snapshot, counts });
  return { prose, structured, reason: detection.reason, snapshot, detection };
}

function logInspection({ tenantId, detection, snapshot, counts, refused }) {
  // No prospect data, names, or mission objectives — route, counts, sources only.
  const row = {
    event: 'OPERATIONAL_INSPECTION',
    spec: SPEC,
    tenantId: String(tenantId),
    route: 'operational_inspection',
    detection: detection.reason,
    refused: refused || null,
    missionCounts: counts || null,
    sources: snapshot.sources.map((s) => ({ source: s.source, available: s.available, reason: s.reason })),
    mutationBoundary: snapshot.mutationBoundary,
    timestamp: new Date().toISOString(),
  };
  if (process.env.NODE_ENV !== 'test') console.info('[OPERATIONAL_INSPECTION]', JSON.stringify(row));
  return row;
}

module.exports = {
  SPEC,
  STALE_AFTER_DAYS,
  CATEGORIES,
  detectOperationalInspection,
  isOperationalInspectionRequest,
  hasAffirmativeActionCommand,
  checkNamedClientScope,
  tenantBlockers,
  countByCategory,
  refusalTurn,
  readOnlyQueryable,
  classifyMission,
  loadOperationalSnapshot,
  resolveNamedClientScope,
  composeOperationalReport,
  maybeHandleOperationalInspectionTurn,
};
