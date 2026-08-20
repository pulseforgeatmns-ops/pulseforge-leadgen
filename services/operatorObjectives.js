'use strict';

/**
 * SPEC-095 — Max Durable Operator Objectives (v1 thin slice).
 *
 * Persist → Retrieve → Resolve → Interpret → Route.
 * Strategic objectives are durable interpretive context only.
 * Creating or updating an objective never executes a Mission, publishes
 * content, sends outreach, or mutates CRM state.
 */

const crypto = require('crypto');

const SCOPES = Object.freeze(['operator', 'client']);
const STATUSES = Object.freeze(['active', 'paused', 'completed', 'cancelled']);
const RESOLUTION = Object.freeze({
  RESOLVED: 'resolved',
  AMBIGUOUS: 'ambiguous',
  UNRESOLVED: 'unresolved',
});

class OperatorObjectiveError extends Error {
  /**
   * @param {string} code
   * @param {string} message
   * @param {number} [status]
   */
  constructor(code, message, status = 400) {
    super(message);
    this.name = 'OperatorObjectiveError';
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

function asText(value) {
  if (value == null) return null;
  const s = String(value).trim();
  return s || null;
}

function asClientId(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.trunc(n);
}

function clone(obj) {
  return JSON.parse(JSON.stringify(obj));
}

function normalizeToken(text) {
  return String(text || '')
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function significantTokens(text) {
  const stop = new Set([
    'the',
    'a',
    'an',
    'and',
    'or',
    'of',
    'for',
    'to',
    'with',
    'our',
    'we',
    'are',
    'is',
    'in',
    'on',
    'at',
    'that',
    'this',
    'next',
    'what',
    'where',
    'how',
    'about',
    'should',
    'publish',
    'campaign',
    'objective',
    'priority',
  ]);
  return normalizeToken(text)
    .split(' ')
    .filter((t) => t && t.length > 2 && !stop.has(t));
}

function normalizeAliases(aliases) {
  if (!Array.isArray(aliases)) return [];
  const out = [];
  const seen = new Set();
  for (const a of aliases) {
    const n = normalizeToken(a);
    if (!n || seen.has(n)) continue;
    seen.add(n);
    out.push(n);
  }
  return out;
}

function assertScope(scope, clientId) {
  if (!SCOPES.includes(scope)) {
    throw new OperatorObjectiveError(
      'invalid_scope',
      `scope must be one of: ${SCOPES.join(', ')}`
    );
  }
  if (scope === 'operator' && clientId != null) {
    throw new OperatorObjectiveError(
      'invalid_scope',
      'operator-scoped objectives must not set client_id'
    );
  }
  if (scope === 'client' && clientId == null) {
    throw new OperatorObjectiveError(
      'invalid_scope',
      'client-scoped objectives require client_id'
    );
  }
}

function assertStatus(status) {
  if (!STATUSES.includes(status)) {
    throw new OperatorObjectiveError(
      'invalid_status',
      `status must be one of: ${STATUSES.join(', ')}`
    );
  }
}

function toPublicObjective(row) {
  if (!row) return null;
  return {
    id: row.id,
    tenantId: row.tenantId,
    scope: row.scope,
    clientId: row.clientId == null ? null : row.clientId,
    title: row.title,
    objectiveText: row.objectiveText,
    status: row.status,
    timeHorizon: row.timeHorizon || null,
    currentPhase: row.currentPhase || null,
    context: row.context && typeof row.context === 'object' ? row.context : {},
    aliases: Array.isArray(row.aliases) ? row.aliases.slice() : [],
    createdBy: row.createdBy || null,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}

function envelopeObjective(row) {
  const o = toPublicObjective(row);
  if (!o) return null;
  return {
    id: o.id,
    scope: o.scope,
    clientId: o.clientId,
    title: o.title,
    objectiveText: o.objectiveText,
    status: o.status,
    currentPhase: o.currentPhase,
    timeHorizon: o.timeHorizon,
    aliases: o.aliases,
  };
}

/* -------------------------------------------------------------------------- */
/* Persistence stores                                                          */
/* -------------------------------------------------------------------------- */

function createMemoryStore() {
  /** @type {Map<string, object>} */
  const rows = new Map();

  return {
    kind: 'memory',
    async insert(row) {
      rows.set(row.id, clone(row));
      return clone(row);
    },
    async update(row) {
      if (!rows.has(row.id)) return null;
      rows.set(row.id, clone(row));
      return clone(row);
    },
    async getById(id, tenantId) {
      const row = rows.get(id);
      if (!row) return null;
      if (tenantId != null && String(row.tenantId) !== String(tenantId)) {
        return null;
      }
      return clone(row);
    },
    async listActive(filter = {}) {
      let list = [...rows.values()].filter((r) => r.status === 'active');
      if (filter.tenantId != null) {
        list = list.filter((r) => String(r.tenantId) === String(filter.tenantId));
      }
      if (filter.clientId != null) {
        const cid = asClientId(filter.clientId);
        list = list.filter(
          (r) =>
            r.scope === 'operator' ||
            (r.scope === 'client' && r.clientId === cid)
        );
      } else if (filter.scope === 'operator') {
        list = list.filter((r) => r.scope === 'operator');
      } else if (filter.scope === 'client') {
        list = list.filter((r) => r.scope === 'client');
      }
      list.sort(
        (a, b) => new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime()
      );
      if (filter.limit != null) list = list.slice(0, filter.limit);
      return list.map(clone);
    },
    async listByTenant(tenantId, opts = {}) {
      let list = [...rows.values()].filter(
        (r) => String(r.tenantId) === String(tenantId)
      );
      if (opts.status) {
        list = list.filter((r) => r.status === opts.status);
      }
      list.sort(
        (a, b) => new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime()
      );
      return list.map(clone);
    },
    clear() {
      rows.clear();
    },
  };
}

function mapRow(row) {
  if (!row) return null;
  return {
    id: row.id,
    tenantId: row.tenant_id,
    scope: row.scope,
    clientId: row.client_id == null ? null : Number(row.client_id),
    title: row.title,
    objectiveText: row.objective_text,
    status: row.status,
    timeHorizon: row.time_horizon || null,
    currentPhase: row.current_phase || null,
    context: row.context || {},
    aliases: Array.isArray(row.aliases) ? row.aliases : [],
    createdBy: row.created_by || null,
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
    async insert(row) {
      const result = await db.query(
        `INSERT INTO operator_objectives (
          id, tenant_id, scope, client_id, title, objective_text, status,
          time_horizon, current_phase, context, aliases, created_by,
          created_at, updated_at
        ) VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11,$12,$13,$14
        ) RETURNING *`,
        [
          row.id,
          row.tenantId,
          row.scope,
          row.clientId,
          row.title,
          row.objectiveText,
          row.status,
          row.timeHorizon,
          row.currentPhase,
          JSON.stringify(row.context || {}),
          row.aliases || [],
          row.createdBy,
          row.createdAt,
          row.updatedAt,
        ]
      );
      return mapRow(result.rows[0]);
    },
    async update(row) {
      const result = await db.query(
        `UPDATE operator_objectives SET
          title = $2,
          objective_text = $3,
          status = $4,
          time_horizon = $5,
          current_phase = $6,
          context = $7::jsonb,
          aliases = $8,
          updated_at = $9
        WHERE id = $1 AND tenant_id = $10
        RETURNING *`,
        [
          row.id,
          row.title,
          row.objectiveText,
          row.status,
          row.timeHorizon,
          row.currentPhase,
          JSON.stringify(row.context || {}),
          row.aliases || [],
          row.updatedAt,
          row.tenantId,
        ]
      );
      return mapRow(result.rows[0] || null);
    },
    async getById(id, tenantId) {
      const result = await db.query(
        `SELECT * FROM operator_objectives WHERE id = $1 AND tenant_id = $2`,
        [id, String(tenantId)]
      );
      return mapRow(result.rows[0] || null);
    },
    async listActive(filter = {}) {
      const params = [];
      const clauses = [`status = 'active'`];
      if (filter.tenantId != null) {
        params.push(String(filter.tenantId));
        clauses.push(`tenant_id = $${params.length}`);
      }
      if (filter.clientId != null) {
        params.push(asClientId(filter.clientId));
        clauses.push(
          `(scope = 'operator' OR (scope = 'client' AND client_id = $${params.length}))`
        );
      }
      params.push(filter.limit != null ? Number(filter.limit) : 50);
      const result = await db.query(
        `SELECT * FROM operator_objectives
         WHERE ${clauses.join(' AND ')}
         ORDER BY updated_at DESC
         LIMIT $${params.length}`,
        params
      );
      return result.rows.map(mapRow);
    },
  };
}

function resolveStore(opts = {}) {
  if (opts.store) return opts.store;
  if (opts.pool) return createPostgresStore(opts.pool);
  if (opts.memory === true || process.env.OPERATOR_OBJECTIVES_STORE === 'memory') {
    return createMemoryStore();
  }
  if (process.env.DATABASE_URL) {
    try {
      return createPostgresStore(opts.pool);
    } catch (_) {
      return createMemoryStore();
    }
  }
  return createMemoryStore();
}

function scheduleOperatorContextRebuildForObjective(objective) {
  if (!objective) return;
  const clientId =
    objective.clientId != null
      ? objective.clientId
      : Number.isFinite(Number(objective.tenantId))
        ? Number(objective.tenantId)
        : null;
  if (clientId == null) return;
  try {
    const { onOperatorObjectiveChanged } = require('./operatorContextEvents');
    onOperatorObjectiveChanged(clientId, { objectiveId: objective.id });
  } catch (_) {
    /* fail soft */
  }
}

/* -------------------------------------------------------------------------- */
/* CRUD                                                                        */
/* -------------------------------------------------------------------------- */

/**
 * @param {object} input
 * @param {object} [opts]
 */
async function createOperatorObjective(input = {}, opts = {}) {
  const tenantId = asText(input.tenantId ?? input.tenant_id);
  if (!tenantId) {
    throw new OperatorObjectiveError('tenant_required', 'tenantId is required');
  }

  const scope = asText(input.scope) || 'operator';
  const clientId =
    scope === 'client'
      ? asClientId(input.clientId ?? input.client_id)
      : null;
  assertScope(scope, clientId);

  const title = asText(input.title);
  const objectiveText = asText(input.objectiveText ?? input.objective_text);
  if (!title) {
    throw new OperatorObjectiveError('title_required', 'title is required');
  }
  if (!objectiveText) {
    throw new OperatorObjectiveError(
      'objective_required',
      'objective_text is required'
    );
  }

  const status = asText(input.status) || 'active';
  assertStatus(status);

  const now = nowIso();
  const row = {
    id: asText(input.id) || newId(),
    tenantId,
    scope,
    clientId,
    title,
    objectiveText,
    status,
    timeHorizon: asText(input.timeHorizon ?? input.time_horizon),
    currentPhase: asText(input.currentPhase ?? input.current_phase),
    context:
      input.context && typeof input.context === 'object' ? clone(input.context) : {},
    aliases: normalizeAliases(input.aliases),
    createdBy: asText(input.createdBy ?? input.created_by),
    createdAt: now,
    updatedAt: now,
  };

  // Derive default aliases from title when none provided.
  if (!row.aliases.length) {
    row.aliases = normalizeAliases([title, ...significantTokens(title)]);
  }

  const store = resolveStore(opts);
  const saved = await store.insert(row);
  scheduleOperatorContextRebuildForObjective(saved);
  return toPublicObjective(saved);
}

/**
 * @param {string} id
 * @param {object} patch
 * @param {object} [opts]
 */
async function updateOperatorObjective(id, patch = {}, opts = {}) {
  const objectiveId = asText(id);
  const tenantId = asText(patch.tenantId ?? patch.tenant_id);
  if (!objectiveId) {
    throw new OperatorObjectiveError('id_required', 'id is required');
  }
  if (!tenantId) {
    throw new OperatorObjectiveError('tenant_required', 'tenantId is required');
  }

  const store = resolveStore(opts);
  const existing = await store.getById(objectiveId, tenantId);
  if (!existing) {
    throw new OperatorObjectiveError('not_found', 'objective not found', 404);
  }

  const next = clone(existing);
  if (patch.title != null) next.title = asText(patch.title) || next.title;
  if (patch.objectiveText != null || patch.objective_text != null) {
    next.objectiveText =
      asText(patch.objectiveText ?? patch.objective_text) || next.objectiveText;
  }
  if (patch.status != null) {
    assertStatus(String(patch.status));
    next.status = String(patch.status);
  }
  if (patch.timeHorizon !== undefined || patch.time_horizon !== undefined) {
    next.timeHorizon = asText(patch.timeHorizon ?? patch.time_horizon);
  }
  if (patch.currentPhase !== undefined || patch.current_phase !== undefined) {
    next.currentPhase = asText(patch.currentPhase ?? patch.current_phase);
  }
  if (patch.context && typeof patch.context === 'object') {
    next.context = { ...(next.context || {}), ...clone(patch.context) };
  }
  if (patch.aliases != null) {
    next.aliases = normalizeAliases(patch.aliases);
  }
  next.updatedAt = nowIso();

  const saved = await store.update(next);
  scheduleOperatorContextRebuildForObjective(saved);
  return toPublicObjective(saved);
}

/**
 * @param {object} filter
 * @param {object} [opts]
 */
async function getActiveObjectives(filter = {}, opts = {}) {
  const tenantId = asText(filter.tenantId ?? filter.tenant_id);
  if (!tenantId) {
    throw new OperatorObjectiveError('tenant_required', 'tenantId is required');
  }
  const store = resolveStore(opts);
  const rows = await store.listActive({
    tenantId,
    clientId: filter.clientId ?? filter.client_id,
    limit: filter.limit != null ? Number(filter.limit) : 25,
  });
  return rows.map(toPublicObjective);
}

/**
 * @param {string} id
 * @param {object} [opts]
 */
async function getObjectiveById(id, opts = {}) {
  const objectiveId = asText(id);
  const tenantId = asText(opts.tenantId ?? opts.tenant_id);
  if (!objectiveId || !tenantId) return null;
  const store = resolveStore(opts);
  return toPublicObjective(await store.getById(objectiveId, tenantId));
}

/* -------------------------------------------------------------------------- */
/* Reference resolution (deterministic, fail-closed)                           */
/* -------------------------------------------------------------------------- */

function objectiveMatchScore(messageNorm, objective) {
  const candidates = normalizeAliases([
    objective.title,
    ...(objective.aliases || []),
  ]);
  let best = 0;
  for (const alias of candidates) {
    if (!alias) continue;
    if (messageNorm === alias) {
      best = Math.max(best, 1);
      continue;
    }
    if (messageNorm.includes(alias) || alias.includes(messageNorm)) {
      best = Math.max(best, 0.92);
      continue;
    }
    // Token overlap on significant title tokens
    const aliasTokens = significantTokens(alias);
    const msgTokens = new Set(significantTokens(messageNorm));
    if (!aliasTokens.length) continue;
    const hit = aliasTokens.filter((t) => msgTokens.has(t)).length;
    const ratio = hit / aliasTokens.length;
    if (ratio >= 1) best = Math.max(best, 0.88);
    else if (ratio >= 0.66 && hit >= 2) best = Math.max(best, 0.75);
  }

  // Soft cue: "the launch" / "launch" against titles containing launch
  if (/\blaunch\b/.test(messageNorm) && /\blaunch\b/.test(normalizeToken(objective.title))) {
    best = Math.max(best, 0.7);
  }
  if (/\bboston\b/.test(messageNorm) && /\bboston\b/.test(normalizeToken(objective.title))) {
    best = Math.max(best, 0.85);
  }
  if (/\baji\b/.test(messageNorm) && /\baji\b/.test(normalizeToken(objective.title))) {
    best = Math.max(best, 0.85);
  }

  return best;
}

/**
 * Resolve a message reference against active objectives.
 * Fail closed: ambiguous or unresolved never invents a match.
 *
 * @param {object} input
 * @param {string} input.message
 * @param {object[]} input.objectives
 * @param {object} [input.scope]
 * @returns {{ status: string, objective: object|null, matches: object[], confidence: number }}
 */
function resolveObjectiveReference(input = {}) {
  const message = asText(input.message) || '';
  const objectives = Array.isArray(input.objectives) ? input.objectives : [];
  if (!message || !objectives.length) {
    return {
      status: RESOLUTION.UNRESOLVED,
      objective: null,
      matches: [],
      confidence: 0,
    };
  }

  const messageNorm = normalizeToken(message);
  const scored = objectives
    .map((o) => ({
      objective: o,
      score: objectiveMatchScore(messageNorm, o),
    }))
    .filter((x) => x.score >= 0.7)
    .sort((a, b) => b.score - a.score);

  if (!scored.length) {
    return {
      status: RESOLUTION.UNRESOLVED,
      objective: null,
      matches: [],
      confidence: 0,
    };
  }

  const top = scored[0];
  const close = scored.filter((x) => top.score - x.score < 0.08 && x.score >= 0.7);

  // Scope ambiguity: operator + client both matching same reference
  if (close.length > 1) {
    return {
      status: RESOLUTION.AMBIGUOUS,
      objective: null,
      matches: close.map((c) => toPublicObjective(c.objective) || c.objective),
      confidence: top.score,
    };
  }

  return {
    status: RESOLUTION.RESOLVED,
    objective: toPublicObjective(top.objective) || top.objective,
    matches: [toPublicObjective(top.objective) || top.objective],
    confidence: top.score,
  };
}

/* -------------------------------------------------------------------------- */
/* Intent detection (deterministic, fail-closed)                               */
/* -------------------------------------------------------------------------- */

/** SPEC-126 — execution verbs block objective persistence. */
const OBJECTIVE_EXECUTION_VERB_RE =
  /\b(create|resume|begin|operate|execute|manage|run|continue|complete)\b/i;

function objectiveHasExecutionLanguage(text) {
  return OBJECTIVE_EXECUTION_VERB_RE.test(asText(text).toLowerCase());
}

/**
 * SPEC-126 — explicit persistence phrasing only.
 * @param {string} text
 * @returns {boolean}
 */
function isExplicitObjectivePersistenceRequest(text) {
  const lower = asText(text).toLowerCase();
  if (!lower) return false;
  return (
    /\bremember\s+my\s+goal\b/.test(lower) ||
    /\bsave\s+(?:this\s+)?(?:as\s+)?(?:an\s+)?(?:active\s+)?objective\b/.test(lower) ||
    /\btrack\s+(?:this\s+)?objective\b/.test(lower) ||
    /\bmake\s+this\s+my\s+current\s+priority\b/.test(lower) ||
    /\bpersist\s+(?:this\s+)?(?:as\s+)?(?:an\s+)?(?:active\s+)?objective\b/.test(lower)
  );
}

/**
 * Strong cues that the operator is establishing a durable objective.
 * Weak / speculative language returns null (do not persist).
 *
 * @param {string} text
 * @returns {object|null}
 */
function detectObjectiveEstablishment(text) {
  const raw = asText(text);
  if (!raw) return null;
  const lower = raw.toLowerCase();

  // SPEC-126 — execution requests never persist objectives.
  if (objectiveHasExecutionLanguage(raw)) return null;

  // Explicitly reject weak speculation
  if (
    /\bcould\s+be\s+interesting\s+someday\b/.test(lower) ||
    /\bshould\s+probably\b/.test(lower) ||
    /\beventually\b/.test(lower) ||
    /\bsomeday\b/.test(lower) ||
    /\bmaybe\b/.test(lower)
  ) {
    return null;
  }

  const establishCue =
    /\bremember\s+my\s+goal\b/.test(lower) ||
    /\btrack\s+(?:this\s+)?objective\b/.test(lower) ||
    /\bmake\s+this\s+my\s+current\s+priority\b/.test(lower) ||
    /\bour\s+objective\s+is\b/.test(lower) ||
    /\bi\s+want\s+you\s+to\s+own\b/.test(lower) ||
    /\bmake\s+this\s+(an\s+)?active\s+priority\b/.test(lower) ||
    /\bwe'?re\s+preparing\s+for\s+(your\s+|the\s+)?(public\s+)?(max\s+)?launch\b/.test(
      lower
    ) ||
    /\bwe'?re\s+launching\b/.test(lower) ||
    /\bwe\s+are\s+launching\b/.test(lower) ||
    /\bwe'?re\s+expanding\b/.test(lower) ||
    /\bwe\s+are\s+expanding\b/.test(lower) ||
    /\bwe'?re\s+going\s+forward\s+with\b/.test(lower) ||
    /\bour\s+priority\s+for\b/.test(lower) ||
    /\bpersist\s+(this\s+)?(as\s+)?(an\s+)?(active\s+)?objective\b/.test(lower) ||
    /\bsave\s+(this\s+)?(as\s+)?(an\s+)?(active\s+)?objective\b/.test(lower);

  if (!establishCue) return null;

  // Prefer operator scope unless message clearly names a client campaign
  let scope = 'operator';
  let title = null;
  let clientHint = null;

  if (/\b(public\s+)?max\s+launch\b|\bpublic\s+launch\b/.test(lower)) {
    title = 'Public Max Launch';
  } else if (/\bboston\b/.test(lower) && /\bexpand/.test(lower)) {
    title = 'Boston Expansion';
  } else if (/\baji\b/.test(lower) && /\bonboard/.test(lower)) {
    title = 'Aji Pilot Onboarding';
    scope = 'client';
    clientHint = 'aji';
  } else if (/\banchor\b/.test(lower) && /\bboston\b/.test(lower)) {
    title = 'Anchor Boston Expansion';
    scope = 'client';
    clientHint = 'anchor';
  }

  if (!title) {
    const m =
      raw.match(/\bour\s+objective\s+is\s+to\s+(.+?)(?:\.|$)/i) ||
      raw.match(/\bpriority\s+for\s+[^:]+:\s*(.+?)(?:\.|$)/i);
    if (m) {
      const snippet = asText(m[1]);
      title = snippet
        ? snippet.length > 60
          ? `${snippet.slice(0, 57)}…`
          : snippet
        : 'Operator Objective';
    } else {
      title = 'Operator Objective';
    }
  }

  const timeHorizon = extractTimeHorizon(raw);
  const currentPhase = extractPhase(raw);
  const aliases = deriveAliases(title, raw);

  return {
    kind: 'establish',
    scope,
    title,
    objectiveText: extractObjectiveText(raw),
    timeHorizon,
    currentPhase,
    aliases,
    clientHint,
    context: {
      launch_trigger: /\bevidence[- ]gated\b/i.test(raw)
        ? 'evidence_gate'
        : undefined,
      owner: /\bi\s+want\s+you\s+to\s+own\b/i.test(raw) ? 'Max' : undefined,
      content_owner: /\bpaige\b/i.test(raw) ? 'Paige' : undefined,
    },
  };
}

function extractObjectiveText(raw) {
  const m = raw.match(
    /\b(?:the\s+)?objective\s+is\s+to\s+(.+?)(?:\.?\s*(?:i\s+want|paige|timing|horizon)|$)/is
  );
  if (m) return asText(m[1]) || raw;
  return raw.length > 600 ? `${raw.slice(0, 597)}…` : raw;
}

function extractTimeHorizon(raw) {
  if (/\bevidence[- ]gated\b/i.test(raw) && /\bthree\s+weeks\b/i.test(raw)) {
    return 'Roughly three weeks, evidence-gated rather than date-forced.';
  }
  const m = raw.match(/\bover\s+(?:roughly\s+|the\s+next\s+)?([^.,\n]{3,60})/i);
  if (m) return asText(m[1]);
  if (/\bevidence[- ]gated\b/i.test(raw)) {
    return 'Evidence-gated (no fixed calendar date).';
  }
  return null;
}

function extractPhase(raw) {
  if (/\bthesis\b/i.test(raw) || /\bproblem\s+exposure\b/i.test(raw)) {
    return 'Thesis / problem exposure';
  }
  if (/\baudience\s+validation\b/i.test(raw)) return 'Audience validation';
  if (/\bproduct\s+breadcrumbs?\b/i.test(raw)) return 'Product breadcrumbs';
  if (/\bmax\s+reveal\b/i.test(raw) || /\breveal\s+max\b/i.test(raw)) {
    return 'Max reveal';
  }
  if (/\bdemo\s+conversion\b/i.test(raw)) return 'Demo conversion';
  return null;
}

function deriveAliases(title, raw) {
  const aliases = [title];
  const lower = String(raw || '').toLowerCase();
  if (/\blaunch\b/.test(lower)) {
    aliases.push('the launch', 'launch campaign', 'max launch', 'public launch');
  }
  if (/\bboston\b/.test(lower)) {
    aliases.push('boston', 'boston expansion', 'the boston expansion');
  }
  if (/\baji\b/.test(lower)) {
    aliases.push('aji', 'aji rollout', 'aji onboarding');
  }
  return normalizeAliases(aliases);
}

/**
 * Lifecycle change detection — requires clear operator intent.
 *
 * @param {string} text
 * @param {object[]} [objectives]
 * @returns {object|null}
 */
function detectObjectiveLifecycleChange(text, objectives = []) {
  const raw = asText(text);
  if (!raw) return null;
  const lower = raw.toLowerCase();

  let status = null;
  if (/\bput\b.+\bon\s+hold\b/.test(lower) || /\bpause\b/.test(lower)) {
    status = 'paused';
  } else if (/\bwe\s+completed\b/.test(lower) || /\bmark\b.+\bcomplete/.test(lower)) {
    status = 'completed';
  } else if (/\bkill\b/.test(lower) || /\bcancel\b/.test(lower)) {
    status = 'cancelled';
  }
  if (!status) return null;

  const resolution = resolveObjectiveReference({
    message: raw,
    objectives,
  });
  if (resolution.status !== RESOLUTION.RESOLVED) {
    return {
      kind: 'lifecycle',
      status,
      resolution,
    };
  }
  return {
    kind: 'lifecycle',
    status,
    resolution,
    objective: resolution.objective,
  };
}

/**
 * Objective update (phase / horizon / context) — clear change language only.
 *
 * @param {string} text
 * @param {object[]} [objectives]
 * @returns {object|null}
 */
function detectObjectiveUpdate(text, objectives = []) {
  const raw = asText(text);
  if (!raw) return null;
  const lower = raw.toLowerCase();

  const updateCue =
    /\bevidence[- ]gated\b/.test(lower) ||
    /\bdon'?t\s+force\s+the\s+date\b/.test(lower) ||
    /\bmake\s+the\s+launch\s+evidence/.test(lower) ||
    /\bupdate\s+(the\s+)?(objective|phase|horizon)\b/.test(lower) ||
    /\bcurrent\s+phase\s+(is|should\s+be)\b/.test(lower) ||
    /\bset\s+(the\s+)?phase\s+to\b/.test(lower);

  if (!updateCue) return null;

  const resolution = resolveObjectiveReference({
    message: raw,
    objectives,
  });
  if (resolution.status !== RESOLUTION.RESOLVED) {
    return { kind: 'update', resolution };
  }

  const patch = {};
  const horizon = extractTimeHorizon(raw);
  if (horizon) patch.timeHorizon = horizon;
  const phase = extractPhase(raw);
  if (phase) patch.currentPhase = phase;
  if (/\bevidence[- ]gated\b/.test(lower)) {
    patch.context = { launch_trigger: 'evidence_gate' };
  }
  if (!Object.keys(patch).length) return null;

  return {
    kind: 'update',
    resolution,
    objective: resolution.objective,
    patch,
  };
}

/**
 * Status / planning question about an existing objective.
 * Used to prevent Mission Engine campaign_creation collisions.
 *
 * @param {string} text
 * @returns {boolean}
 */
function looksLikeObjectiveStatusRequest(text) {
  const q = String(text || '').trim().toLowerCase();
  if (!q) return false;

  // Explicit new executable campaign requests are NOT status questions
  if (
    /\b(build|create|launch|prepare|new)\s+(a\s+)?(q\d\s+)?(outreach\s+)?campaign\s+(targeting|for|against)\b/.test(
      q
    ) ||
    /\blaunch\s+a\s+.+\bcampaign\b/.test(q) ||
    /\bbuild\s+a\s+\d+[-\s]?company\s+campaign\b/.test(q)
  ) {
    return false;
  }

  return (
    /\bwhere\s+are\s+we\s+with\b/.test(q) ||
    /\bwhat'?s\s+next\s+(for|with|on)\b/.test(q) ||
    /\bwhats\s+next\s+(for|with|on)\b/.test(q) ||
    /\bhow\s+is\s+(the|our)\b/.test(q) ||
    /\bstatus\s+of\b/.test(q) ||
    /\bwhere\s+do\s+we\s+stand\b/.test(q) ||
    /\bprogress\s+on\b/.test(q) ||
    /\bany\s+update\s+on\b/.test(q) ||
    /\bhas\s+anything\s+changed\s+(our|the)\s+plan\b/.test(q)
  );
}

/**
 * Content ask that should reach Paige when a resolved objective exists.
 *
 * @param {string} text
 * @returns {boolean}
 */
function looksLikeObjectiveContentRequest(text) {
  const q = String(text || '').trim().toLowerCase();
  if (!q) return false;
  return (
    /\b(what\s+should\s+(we|i)\s+(publish|post)|publish\s+next|next\s+(post|experiment|content)|content\s+(strategy|experiment|recommendation)|ask\s+paige)\b/.test(
      q
    ) ||
    (/\bpublish\b/.test(q) && /\b(launch|campaign|next)\b/.test(q))
  );
}

/**
 * True when cold intent looks like campaign_creation but the message is
 * actually about an existing resolved objective (status / content), not a
 * new executable campaign.
 *
 * @param {string} text
 * @param {object|null} resolvedObjective
 * @returns {boolean}
 */
function shouldSuppressMissionForResolvedObjective(text, resolvedObjective) {
  if (!resolvedObjective) return false;
  if (looksLikeObjectiveStatusRequest(text)) return true;
  if (looksLikeObjectiveContentRequest(text)) return true;

  // "… Max launch campaign" without build/create/targeting language
  const q = String(text || '').toLowerCase();
  const explicitNew =
    /\b(build|create|prepare|new)\s+(a\s+)?(outreach\s+)?campaign\b/.test(q) ||
    /\blaunch\s+a\s+.+\bcampaign\s+(targeting|for)\b/.test(q) ||
    /\btargeting\b.+\bin\b/.test(q);
  if (explicitNew) return false;

  // Mentions of the resolved objective title/aliases with planning verbs
  const title = normalizeToken(resolvedObjective.title || '');
  if (title && normalizeToken(q).includes(title)) {
    if (
      /\b(where|status|progress|next|plan|publish|post|content|phase)\b/.test(q)
    ) {
      return true;
    }
  }
  return false;
}

/**
 * Operator-facing status prose — structured objective data is authoritative.
 *
 * @param {object} objective
 * @returns {string}
 */
function formatObjectiveStatusResponse(objective) {
  const parts = [];
  parts.push(`ACTIVE OBJECTIVE`);
  parts.push('');
  parts.push(objective.title);
  parts.push(`Status: ${objective.status}`);
  if (objective.currentPhase) {
    parts.push(`Phase: ${objective.currentPhase}`);
  }
  if (objective.timeHorizon) {
    parts.push(`Horizon: ${objective.timeHorizon}`);
  }
  if (objective.scope) {
    parts.push(`Scope: ${objective.scope}`);
  }
  parts.push('');
  parts.push(objective.objectiveText);
  const owner =
    (objective.context && (objective.context.owner || objective.context.content_owner)) ||
    null;
  if (owner) {
    parts.push('');
    parts.push(
      `Owner context: ${
        objective.context.owner ? `Max owns overall. ` : ''
      }${
        objective.context.content_owner
          ? `${objective.context.content_owner} owns content strategy.`
          : ''
      }`.trim()
    );
  }
  parts.push('');
  parts.push(
    'This is standing strategic context — not a new Mission and not execution permission.'
  );
  return parts.join('\n');
}

/**
 * Confirmation after persisting an objective.
 *
 * @param {object} objective
 * @returns {string}
 */
function formatObjectiveCreatedResponse(objective) {
  const parts = [
    `I've saved ${objective.title} as an active ${objective.scope} objective.`,
    '',
  ];
  if (objective.currentPhase) {
    parts.push(`Current phase: ${objective.currentPhase}`);
  }
  if (objective.timeHorizon) {
    parts.push(`Timing: ${objective.timeHorizon}`);
  }
  parts.push('');
  parts.push(
    'I will recover this objective in fresh conversations before interpreting related requests. Creating it does not start a Mission or execute any work.'
  );
  return parts.join('\n');
}

/**
 * Confirmation after updating an objective.
 *
 * @param {object} before
 * @param {object} after
 * @returns {string}
 */
function formatObjectiveUpdatedResponse(before, after) {
  const parts = [`Updated ${after.title}:`, ''];
  if (before.timeHorizon !== after.timeHorizon) {
    parts.push(`Timing: ${before.timeHorizon || '—'} → ${after.timeHorizon || '—'}`);
  }
  if (before.currentPhase !== after.currentPhase) {
    parts.push(
      `Phase: ${before.currentPhase || '—'} → ${after.currentPhase || '—'}`
    );
  }
  if (before.status !== after.status) {
    parts.push(`Status: ${before.status} → ${after.status}`);
  }
  return parts.join('\n');
}

/**
 * Ambiguity clarification — fail closed.
 *
 * @param {object[]} matches
 * @returns {string}
 */
function formatAmbiguousObjectiveResponse(matches) {
  const lines = [
    'I have more than one active objective that could match that reference:',
    '',
  ];
  for (const m of matches) {
    const scopeLabel =
      m.scope === 'operator'
        ? 'Operator'
        : `Client${m.clientId != null ? ` ${m.clientId}` : ''}`;
    lines.push(`- ${scopeLabel}: ${m.title}`);
  }
  lines.push('');
  lines.push('Which one do you mean?');
  return lines.join('\n');
}

/**
 * Attach normalized objective context for ContextEnvelope (carrier only).
 *
 * @param {object[]} objectives
 * @param {object|null} resolved
 * @param {string} resolutionStatus
 * @returns {object}
 */
function buildObjectiveContextAttachment(objectives, resolved, resolutionStatus) {
  return {
    activeObjectives: (objectives || []).map(envelopeObjective).filter(Boolean),
    resolvedObjective: resolved ? envelopeObjective(resolved) : null,
    objectiveResolution: resolutionStatus || RESOLUTION.UNRESOLVED,
  };
}

/**
 * Seed helper for tests / local — Public Max Launch fixture.
 *
 * @param {object} [opts]
 */
async function ensurePublicMaxLaunchObjective(opts = {}) {
  const tenantId = asText(opts.tenantId) || '1';
  const store = resolveStore(opts);
  const existing = await store.listActive({ tenantId, limit: 50 });
  const found = existing.find(
    (o) =>
      o.scope === 'operator' &&
      normalizeToken(o.title) === 'public max launch'
  );
  if (found) return toPublicObjective(found);

  return createOperatorObjective(
    {
      tenantId,
      scope: 'operator',
      title: 'Public Max Launch',
      objectiveText:
        'Build qualified attention around the ideas behind Pulseforge, progressively expose the problems we\'re solving, then reveal Max and convert that attention into qualified demos.',
      status: 'active',
      timeHorizon: 'Roughly three weeks, evidence-gated rather than date-forced.',
      currentPhase: 'Thesis / problem exposure',
      aliases: [
        'max launch',
        'public max launch',
        'the launch',
        'launch campaign',
        'max launch campaign',
        'public launch',
      ],
      context: {
        owner: 'Max',
        content_owner: 'Paige',
        launch_trigger: 'evidence_gate',
        notes:
          'Max owns the overall campaign. Paige owns content strategy and individual content experiments.',
      },
      createdBy: 'spec_095_seed',
    },
    { store }
  );
}

module.exports = {
  SCOPES,
  STATUSES,
  RESOLUTION,
  OperatorObjectiveError,
  createMemoryStore,
  createPostgresStore,
  createOperatorObjective,
  updateOperatorObjective,
  getActiveObjectives,
  getObjectiveById,
  resolveObjectiveReference,
  detectObjectiveEstablishment,
  isExplicitObjectivePersistenceRequest,
  detectObjectiveLifecycleChange,
  detectObjectiveUpdate,
  looksLikeObjectiveStatusRequest,
  looksLikeObjectiveContentRequest,
  shouldSuppressMissionForResolvedObjective,
  formatObjectiveStatusResponse,
  formatObjectiveCreatedResponse,
  formatObjectiveUpdatedResponse,
  formatAmbiguousObjectiveResponse,
  buildObjectiveContextAttachment,
  ensurePublicMaxLaunchObjective,
  envelopeObjective,
  toPublicObjective,
  normalizeToken,
  significantTokens,
};
