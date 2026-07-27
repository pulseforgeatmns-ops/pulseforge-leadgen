'use strict';

const {
  SUBJECT_FIELD_ALIASES,
  CONFIDENCE_FIELD_ALIASES,
  ID_FIELD_ALIASES,
} = require('./types');
const { parseEql } = require('./Parser');
const { planEql } = require('./QueryPlanner');

/**
 * EQL Executor — run a planned query against a domain-neutral catalog (SPEC-020).
 *
 * No CRM / Market branching. Subject identity is resolved via field aliases.
 */

/**
 * @typedef {object} EvidenceCatalog
 * @property {(target: import('./types').EqlTarget) => object[]|Promise<object[]>} list
 * @property {(target: import('./types').EqlTarget, id: string) => object|null|Promise<object|null>} [get]
 * @property {(args: { target: import('./types').EqlTarget, relation: string, related: import('./types').EqlEntityRef }) => object[]|Promise<object[]>} [relate]
 * @property {(args: { subject: string|null, from: string, to: string }) => object|object[]|Promise<object|object[]>} [replay]
 * @property {(args: { left: unknown, right: unknown }) => object|Promise<object>} [compare]
 * @property {(args: { entity: import('./types').EqlEntityRef|null, row?: object|null, rows?: object[] }) => object|Promise<object>} [explain]
 */

class Executor {
  /**
   * @param {object} [deps]
   * @param {EvidenceCatalog} [deps.catalog]
   */
  constructor(deps = {}) {
    this._catalog = deps.catalog || createEvidenceCatalog();
  }

  /**
   * Parse → plan → execute.
   * @param {string} source
   * @returns {Promise<import('./types').EqlResult>}
   */
  async query(source) {
    const ast = parseEql(source);
    const plan = planEql(ast);
    return this.execute(plan);
  }

  /**
   * @param {import('./types').EqlPlan} plan
   * @returns {Promise<import('./types').EqlResult>}
   */
  async execute(plan) {
    if (!plan || !plan.steps) {
      throw new Error('Executor.execute requires a plan');
    }

    /** @type {object[]} */
    let rows = [];
    /** @type {object|null} */
    let explanation = null;
    /** @type {import('./types').EqlTarget|null} */
    let target = null;

    for (const step of plan.steps) {
      switch (step.op) {
        case 'scan': {
          target = /** @type {import('./types').EqlTarget} */ (step.args.target);
          const entity = step.args.entity || null;
          rows = await this._scan(target, entity);
          break;
        }
        case 'filter': {
          rows = rows.filter((row) =>
            matchesAll(row, /** @type {import('./types').EqlCondition[]} */ (step.args.where))
          );
          break;
        }
        case 'sort': {
          rows = sortRows(rows, /** @type {import('./types').EqlOrderBy} */ (step.args.orderBy));
          break;
        }
        case 'limit': {
          rows = rows.slice(0, /** @type {number} */ (step.args.limit));
          break;
        }
        case 'relate': {
          target = /** @type {import('./types').EqlTarget} */ (step.args.target);
          rows = await this._relate({
            target,
            relation: String(step.args.relation),
            related: /** @type {import('./types').EqlEntityRef} */ (step.args.related),
          });
          break;
        }
        case 'replay': {
          target = 'replay_sessions';
          const replayed = await this._replay({
            subject: step.args.subject == null ? null : String(step.args.subject),
            from: String(step.args.from),
            to: String(step.args.to),
          });
          rows = Array.isArray(replayed) ? replayed : [replayed];
          break;
        }
        case 'compare': {
          target = null;
          const compared = await this._compare({
            left: step.args.left,
            right: step.args.right,
          });
          rows = [compared];
          break;
        }
        case 'explain_entity': {
          explanation = await this._explain({
            entity: /** @type {import('./types').EqlEntityRef|null} */ (step.args.entity),
            rows,
          });
          if (step.args.entity) {
            const found = await this._scan(step.args.entity.target, step.args.entity);
            rows = found;
            target = step.args.entity.target;
          }
          break;
        }
        case 'explain': {
          explanation = await this._explain({
            entity: /** @type {import('./types').EqlEntityRef|null} */ (step.args.entity || null),
            row: rows[0] || null,
            rows,
          });
          break;
        }
        case 'project':
          break;
        default:
          throw new Error(`Unknown plan op: ${step.op}`);
      }
    }

    return Object.freeze({
      kind: plan.kind,
      target,
      rows: Object.freeze(rows.slice()),
      count: rows.length,
      explanation: explanation ? Object.freeze(explanation) : null,
      ast: plan.ast,
      plan,
      mutatesProduction: false,
    });
  }

  /**
   * @param {import('./types').EqlTarget} target
   * @param {import('./types').EqlEntityRef|null} entity
   */
  async _scan(target, entity) {
    if (entity) {
      if (typeof this._catalog.get === 'function') {
        const one = await this._catalog.get(entity.target, entity.id);
        return one ? [one] : [];
      }
      const all = await this._catalog.list(entity.target);
      return all.filter((row) => rowMatchesId(row, entity.id));
    }
    return (await this._catalog.list(target)).slice();
  }

  /**
   * @param {{ target: import('./types').EqlTarget, relation: string, related: import('./types').EqlEntityRef }} args
   */
  async _relate(args) {
    if (typeof this._catalog.relate === 'function') {
      return (await this._catalog.relate(args)).slice();
    }
    const relatedId = args.related.id;
    const relation = String(args.relation).toUpperCase();
    if (relation === 'FOR') {
      const candidates = await this._catalog.list(args.target);
      return candidates.filter((row) => relatesFor(row, args.related));
    }
    // Default: filter evidence/observations whose claim refs match.
    const candidates = await this._catalog.list(args.target);
    return candidates.filter((row) => relatesToClaim(row, args.relation, relatedId));
  }

  /**
   * @param {{ subject: string|null, from: string, to: string }} args
   */
  async _replay(args) {
    if (typeof this._catalog.replay === 'function') {
      return this._catalog.replay(args);
    }
    const sessions = await this._catalog.list('replay_sessions');
    return sessions.filter((session) => {
      if (args.subject != null && resolveSubject(session) !== args.subject) {
        return false;
      }
      const start = session.startTime || session.from || session.start || null;
      const end = session.endTime || session.to || session.end || null;
      if (start && String(start) < args.from) return false;
      if (end && String(end) > args.to) return false;
      return true;
    });
  }

  /**
   * @param {{ left: unknown, right: unknown }} args
   */
  async _compare(args) {
    if (typeof this._catalog.compare === 'function') {
      return this._catalog.compare(args);
    }
    const left = await resolveCompareSide(this._catalog, args.left);
    const right = await resolveCompareSide(this._catalog, args.right);
    return Object.freeze({
      left,
      right,
      leftId: sideId(args.left, left),
      rightId: sideId(args.right, right),
      equal: stableStringify(left) === stableStringify(right),
    });
  }

  /**
   * @param {{ entity: import('./types').EqlEntityRef|null, row?: object|null, rows?: object[] }} args
   */
  async _explain(args) {
    if (typeof this._catalog.explain === 'function') {
      return this._catalog.explain(args);
    }
    return defaultExplain(args);
  }
}

/**
 * In-memory Evidence Catalog — domain-neutral bag of typed records.
 *
 * @param {object} [seed]
 * @param {object[]} [seed.subjects]
 * @param {object[]} [seed.observations]
 * @param {object[]} [seed.evidence]
 * @param {object[]} [seed.claims]
 * @param {object[]} [seed.outcomes]
 * @param {object[]} [seed.recommendations]
 * @param {object[]} [seed.replay_sessions]
 * @param {object[]} [seed.calibrations]
 * @param {object[]} [seed.accuracies]
 * @param {object[]} [seed.strategy_packs]
 * @param {object} [seed.links] - optional { supporting: {claimId: evidence[]}, contradicting: {...} }
 * @param {(args: object) => object|Promise<object>} [seed.replayFn]
 * @param {(args: object) => object|Promise<object>} [seed.compareFn]
 * @param {(args: object) => object|Promise<object>} [seed.explainFn]
 * @param {(args: object) => object[]|Promise<object[]>} [seed.relateForFn]
 * @returns {EvidenceCatalog & { seed: object, add: Function }}
 */
function createEvidenceCatalog(seed = {}) {
  const store = {
    subjects: cloneList(seed.subjects),
    observations: cloneList(seed.observations),
    evidence: cloneList(seed.evidence),
    claims: cloneList(seed.claims),
    outcomes: cloneList(seed.outcomes),
    recommendations: cloneList(seed.recommendations),
    replay_sessions: cloneList(seed.replay_sessions),
    calibrations: cloneList(seed.calibrations),
    accuracies: cloneList(seed.accuracies),
    strategy_packs: cloneList(seed.strategy_packs),
  };
  const links = {
    supporting: { ...(seed.links && seed.links.supporting) },
    contradicting: { ...(seed.links && seed.links.contradicting) },
  };

  /** @type {EvidenceCatalog & { seed: object, add: Function, store: object }} */
  const catalog = {
    store,
    seed: store,
    add(target, row) {
      if (!store[target]) throw new Error(`Unknown catalog target: ${target}`);
      store[target].push(row);
      return catalog;
    },
    async list(target) {
      if (!store[target]) throw new Error(`Unknown catalog target: ${target}`);
      return store[target].slice();
    },
    async get(target, id) {
      const rows = store[target] || [];
      return rows.find((row) => rowMatchesId(row, id)) || null;
    },
    async relate({ target, relation, related }) {
      const key = String(relation).toLowerCase();
      if (key === 'for') {
        return relateFor(store, target, related, seed);
      }
      const bucket = key === 'contradicting' ? links.contradicting : links.supporting;
      const linked = bucket[related.id] || bucket[String(related.id)] || null;
      if (Array.isArray(linked)) {
        return linked.slice();
      }
      const candidates = store[target] || [];
      return candidates.filter((row) => relatesToClaim(row, relation, related.id));
    },
    async replay(args) {
      if (typeof seed.replayFn === 'function') {
        return seed.replayFn(args);
      }
      const sessions = store.replay_sessions.slice();
      return sessions.filter((session) => {
        if (args.subject != null && resolveSubject(session) !== args.subject) {
          return false;
        }
        const start = session.startTime || session.from || null;
        const end = session.endTime || session.to || null;
        if (start && String(start) < args.from) return false;
        if (end && String(end) > args.to) return false;
        // Also include sessions whose window overlaps [from, to]
        return true;
      }).map((session) =>
        Object.freeze({
          ...session,
          window: { from: args.from, to: args.to },
          kind: 'replay_window',
        })
      );
    },
    async compare(args) {
      if (typeof seed.compareFn === 'function') {
        return seed.compareFn(args);
      }
      const left = await resolveCompareSide(catalog, args.left);
      const right = await resolveCompareSide(catalog, args.right);
      return Object.freeze({
        left,
        right,
        leftId: sideId(args.left, left),
        rightId: sideId(args.right, right),
        equal: stableStringify(left) === stableStringify(right),
      });
    },
    async explain(args) {
      if (typeof seed.explainFn === 'function') {
        return seed.explainFn(args);
      }
      return defaultExplain(args);
    },
  };

  return catalog;
}

/**
 * Build a catalog from a replay / laboratory result (domain-neutral projection).
 *
 * @param {object} result
 * @param {object} [extras]
 * @returns {ReturnType<typeof createEvidenceCatalog>}
 */
function catalogFromResult(result, extras = {}) {
  const subjectId = result.subjectId || result.subject || null;
  const claims = normalizeClaims(result.claims).map((claim) => ({
    ...claim,
    subject: claim.subject || subjectId,
    subjectId: claim.subjectId || subjectId,
  }));
  const evidence = normalizeList(result.evidence);
  const observations = normalizeList(result.observations).map((obs) => ({
    ...obs,
    subjectId: obs.subjectId || subjectId,
    subject: obs.subject || obs.subjectId || subjectId,
  }));
  const recommendations = normalizeList(result.recommendations).map((rec) => ({
    ...rec,
    subjectId: rec.subjectId || subjectId,
    subject: rec.subject || rec.subjectId || subjectId,
  }));
  const outcomes = normalizeList(result.outcomes).map((out) => ({
    ...out,
    subjectId: out.subjectId || subjectId,
    subject: out.subject || out.subjectId || subjectId,
  }));

  const subjects = subjectId
    ? [{ id: subjectId, subject: subjectId, subjectId }]
    : [];

  const replay_sessions = [
    {
      id: result.experimentId || result.replayFingerprint || `replay:${subjectId || 'unknown'}`,
      subject: subjectId,
      subjectId,
      startTime: result.startTime || null,
      endTime: result.endTime || null,
      confidence: result.confidence ?? null,
      versions: result.versions || null,
    },
  ];

  const supporting = {};
  const contradicting = {};
  for (const claim of claims) {
    const claimId = claimIdOf(claim);
    if (!claimId) continue;
    supporting[claimId] = collectLinkedEvidence(result, claim, 'supporting');
    contradicting[claimId] = collectLinkedEvidence(result, claim, 'contradicting');
  }

  return createEvidenceCatalog({
    subjects: extras.subjects || subjects,
    observations: extras.observations || observations,
    evidence: extras.evidence || evidence,
    claims: extras.claims || claims,
    outcomes: extras.outcomes || outcomes,
    recommendations: extras.recommendations || recommendations,
    replay_sessions: extras.replay_sessions || replay_sessions,
    links: { supporting, contradicting },
    replayFn: extras.replayFn,
    compareFn: extras.compareFn,
    explainFn:
      extras.explainFn ||
      ((args) =>
        explainFromResult(result, args)),
  });
}

// --- matching / field resolution (domain-neutral) ---

/**
 * @param {object} row
 * @param {import('./types').EqlCondition[]} conditions
 */
function matchesAll(row, conditions) {
  return conditions.every((c) => matchesCondition(row, c));
}

/**
 * @param {object} row
 * @param {import('./types').EqlCondition} condition
 */
function matchesCondition(row, condition) {
  const actual = resolveField(row, condition.field);
  return compareValues(actual, condition.operator, condition.value);
}

/**
 * @param {object} row
 * @param {string} field
 */
function resolveField(row, field) {
  if (!row || typeof row !== 'object') return undefined;
  const key = String(field);

  if (Object.prototype.hasOwnProperty.call(row, key)) return row[key];

  const lower = key.toLowerCase();
  if (lower === 'subject') {
    return firstDefined(row, SUBJECT_FIELD_ALIASES);
  }
  if (lower === 'confidence') {
    const raw = firstDefined(row, CONFIDENCE_FIELD_ALIASES);
    return normalizeConfidence(raw);
  }
  if (lower === 'id') {
    return firstDefined(row, ID_FIELD_ALIASES);
  }

  // case-insensitive own-key fallback
  for (const k of Object.keys(row)) {
    if (k.toLowerCase() === lower) return row[k];
  }

  // nested payload
  if (row.payload && typeof row.payload === 'object') {
    return resolveField(row.payload, field);
  }
  if (row.metadata && typeof row.metadata === 'object') {
    return resolveField(row.metadata, field);
  }
  return undefined;
}

/**
 * @param {object} row
 * @param {string[]} aliases
 */
function firstDefined(row, aliases) {
  for (const key of aliases) {
    if (row[key] != null) return row[key];
  }
  return undefined;
}

/**
 * Confidence may be 0–1 or 0–100; normalize comparisons against query literals.
 * If the row uses 0–100 and the query uses 0–1 (or vice versa), compare in 0–1 space
 * when the query value is ≤ 1 and the row value is > 1.
 */
function normalizeConfidence(raw) {
  if (raw == null) return raw;
  const n = Number(raw);
  return Number.isFinite(n) ? n : raw;
}

/**
 * @param {unknown} actual
 * @param {import('./types').EqlOperator} operator
 * @param {unknown} expected
 */
function compareValues(actual, operator, expected) {
  if (operator === 'CONTAINS') {
    if (actual == null) return false;
    if (Array.isArray(actual)) {
      return actual.map(String).includes(String(expected));
    }
    return String(actual).toLowerCase().includes(String(expected).toLowerCase());
  }

  if (operator === '=' || operator === '!=') {
    const eq = softEqual(actual, expected);
    return operator === '=' ? eq : !eq;
  }

  const a = coerceNumber(actual, expected);
  const b = coerceNumber(expected, actual);
  if (a == null || b == null) return false;

  switch (operator) {
    case '>':
      return a > b;
    case '>=':
      return a >= b;
    case '<':
      return a < b;
    case '<=':
      return a <= b;
    default:
      return false;
  }
}

/**
 * Soft equality: string/number coercion + confidence scale tolerance.
 */
function softEqual(actual, expected) {
  if (actual === expected) return true;
  if (actual == null || expected == null) return actual == expected;
  if (typeof actual === 'number' || typeof expected === 'number') {
    const a = Number(actual);
    const b = Number(expected);
    if (Number.isFinite(a) && Number.isFinite(b)) {
      if (a === b) return true;
      // 0–100 vs 0–1 confidence
      if (Math.abs(a - b * 100) < 1e-9 || Math.abs(a * 100 - b) < 1e-9) return true;
    }
  }
  return String(actual) === String(expected);
}

/**
 * @param {unknown} value
 * @param {unknown} other - peer value for scale hint
 * @returns {number|null}
 */
function coerceNumber(value, other) {
  if (value == null) return null;
  let n = Number(value);
  if (!Number.isFinite(n)) return null;
  const peer = Number(other);
  // Scale 0–100 row values into 0–1 when the query literal is ≤ 1.
  if (Number.isFinite(peer) && peer <= 1 && n > 1 && n <= 100) {
    n = n / 100;
  }
  return n;
}

/**
 * @param {object[]} rows
 * @param {import('./types').EqlOrderBy} orderBy
 */
function sortRows(rows, orderBy) {
  const dir = orderBy.direction === 'DESC' ? -1 : 1;
  return rows.slice().sort((a, b) => {
    const av = resolveField(a, orderBy.field);
    const bv = resolveField(b, orderBy.field);
    if (av == null && bv == null) return 0;
    if (av == null) return 1;
    if (bv == null) return -1;
    const an = Number(av);
    const bn = Number(bv);
    if (Number.isFinite(an) && Number.isFinite(bn)) {
      if (an === bn) return 0;
      return an < bn ? -1 * dir : 1 * dir;
    }
    const as = String(av);
    const bs = String(bv);
    if (as === bs) return 0;
    return as < bs ? -1 * dir : 1 * dir;
  });
}

/**
 * @param {object} row
 * @param {string} id
 */
function rowMatchesId(row, id) {
  const target = String(id);
  for (const key of ID_FIELD_ALIASES) {
    if (row[key] != null && String(row[key]) === target) return true;
  }
  if (row.id != null && String(row.id) === target) return true;
  if (row.statement != null && String(row.statement) === target) return true;
  return false;
}

/**
 * @param {object} row
 * @param {string} relation
 * @param {string} claimId
 */
function relatesToClaim(row, relation, claimId) {
  const target = String(claimId);
  const rel = String(relation).toUpperCase();

  const claimRefs = []
    .concat(row.claimId || [])
    .concat(row.claimIds || [])
    .concat(row.claims || [])
    .concat(row.supports || [])
    .concat(row.supportedClaims || [])
    .map(String);

  const role = String(row.role || row.relation || row.polarity || '').toUpperCase();
  const matchesClaim =
    claimRefs.includes(target) ||
    rowMatchesId(row, target) ||
    (row.claim && rowMatchesId(row.claim, target));

  if (!matchesClaim) {
    // supportingEvidence arrays sometimes embed claim linkage loosely
    if (Array.isArray(row.supportsClaims) && row.supportsClaims.map(String).includes(target)) {
      return rel === 'SUPPORTING' || rel === 'CONTRADICTING'
        ? true
        : true;
    }
    return false;
  }

  if (rel === 'SUPPORTING') {
    if (!role) return true;
    return (
      role === 'SUPPORTING' ||
      role === 'SUPPORT' ||
      role === 'SUPPORTS' ||
      role === 'APPEARED' ||
      role === 'PRESENT' ||
      role === 'SUPPORTING_REF'
    );
  }
  if (rel === 'CONTRADICTING') {
    if (!role) {
      // Without an explicit role, only include rows flagged as opposing
      return Boolean(row.contradicts || row.opposing || row.contradicting);
    }
    return (
      role === 'CONTRADICTING' ||
      role === 'CONTRADICTS' ||
      role === 'OPPOSING' ||
      role === 'AGAINST'
    );
  }
  return true;
}

/**
 * SHOW Calibration FOR Claim("x") / SHOW Accuracy FOR StrategyPack("market")
 * @param {object} row
 * @param {import('./types').EqlEntityRef} related
 */
function relatesFor(row, related) {
  if (!row || !related) return false;
  const id = String(related.id);
  const relatedTarget = related.target;

  if (relatedTarget === 'claims') {
    return (
      rowMatchesId(row, id) ||
      String(row.claimId || '') === id ||
      String(row.claimType || '') === id ||
      String(row.scopeId || '') === id
    );
  }
  if (relatedTarget === 'strategy_packs') {
    return (
      String(row.strategyPack || row.pack || row.scopeId || row.id || '') === id ||
      (row.scope === 'strategy_pack' && String(row.scopeId || '') === id)
    );
  }
  return rowMatchesId(row, id);
}

/**
 * @param {object} store
 * @param {import('./types').EqlTarget} target
 * @param {import('./types').EqlEntityRef} related
 * @param {object} seed
 */
function relateFor(store, target, related, seed) {
  if (typeof seed.relateForFn === 'function') {
    return seed.relateForFn({ target, related });
  }
  const candidates = store[target] || [];
  const filtered = candidates.filter((row) => relatesFor(row, related));
  if (filtered.length > 0) return filtered;

  // Synthesize a projection row when the catalog has claims/strategy stats but
  // no dedicated calibrations/accuracies yet.
  if (target === 'calibrations' && related.target === 'claims') {
    const claim =
      (store.claims || []).find((row) => rowMatchesId(row, related.id)) || null;
    return [
      Object.freeze({
        id: `calibration:${related.id}`,
        claimId: related.id,
        claimType: claim && (claim.claimType || claim.type) || related.id,
        confidence: claim ? claim.confidence ?? null : null,
        historicalCalibration: claim ? claim.historicalCalibration ?? claim.accuracy ?? null : null,
        adjustedConfidence: claim ? claim.adjustedConfidence ?? null : null,
        scope: 'claim',
        scopeId: related.id,
        synthesized: true,
      }),
    ];
  }
  if (target === 'accuracies') {
    if (related.target === 'strategy_packs') {
      const pack =
        (store.strategy_packs || []).find((row) => rowMatchesId(row, related.id)) ||
        null;
      return [
        Object.freeze({
          id: `accuracy:strategy_pack:${related.id}`,
          scope: 'strategy_pack',
          scopeId: related.id,
          strategyPack: related.id,
          accuracy: pack ? pack.accuracy ?? null : null,
          precision: pack ? pack.precision ?? null : null,
          recall: pack ? pack.recall ?? null : null,
          occurrences: pack ? pack.occurrences ?? null : null,
          synthesized: true,
        }),
      ];
    }
    if (related.target === 'claims') {
      const claim =
        (store.claims || []).find((row) => rowMatchesId(row, related.id)) || null;
      return [
        Object.freeze({
          id: `accuracy:claim:${related.id}`,
          scope: 'claim',
          scopeId: related.id,
          claimId: related.id,
          accuracy: claim ? claim.accuracy ?? null : null,
          precision: claim ? claim.precision ?? null : null,
          recall: claim ? claim.recall ?? null : null,
          occurrences: claim ? claim.occurrences ?? null : null,
          synthesized: true,
        }),
      ];
    }
  }
  return filtered;
}

function resolveSubject(row) {
  const value = firstDefined(row, SUBJECT_FIELD_ALIASES);
  return value == null ? null : String(value);
}

/**
 * @param {EvidenceCatalog} catalog
 * @param {unknown} side
 */
async function resolveCompareSide(catalog, side) {
  if (side == null) return null;
  if (typeof side === 'string') {
    // Try claims, then replay sessions, then any target
    for (const target of [
      'claims',
      'replay_sessions',
      'recommendations',
      'evidence',
      'observations',
      'outcomes',
      'subjects',
    ]) {
      if (typeof catalog.get === 'function') {
        const hit = await catalog.get(/** @type {any} */ (target), side);
        if (hit) return hit;
      }
    }
    return { id: side };
  }
  if (typeof side === 'object' && side.target && side.id != null) {
    if (typeof catalog.get === 'function') {
      return (await catalog.get(side.target, side.id)) || side;
    }
  }
  return side;
}

function sideId(side, resolved) {
  if (typeof side === 'string') return side;
  if (side && typeof side === 'object' && side.id != null) return String(side.id);
  if (resolved && resolved.id != null) return String(resolved.id);
  return null;
}

/**
 * @param {{ entity: import('./types').EqlEntityRef|null, row?: object|null, rows?: object[] }} args
 */
function defaultExplain(args) {
  const row = args.row || (args.rows && args.rows[0]) || null;
  const entity = args.entity;
  return Object.freeze({
    entity,
    subject: row ? resolveSubject(row) : entity ? entity.id : null,
    supportingEvidence: (row && (row.supportingEvidence || row.supporting)) || [],
    contradictingEvidence: (row && (row.contradictingEvidence || row.contradicting)) || [],
    confidenceHistory: (row && (row.confidenceHistory || row.confidenceChanges)) || [],
    reasoningTrace: (row && (row.reasoningTrace || row.trace)) || null,
    confidence: row ? resolveField(row, 'confidence') : null,
    row,
  });
}

/**
 * @param {object} result
 * @param {{ entity: import('./types').EqlEntityRef|null, row?: object|null, rows?: object[] }} args
 */
function explainFromResult(result, args) {
  const entity = args.entity;
  const claimId = entity && entity.target === 'claims' ? entity.id : null;
  const explanation = result.explanation || {};
  const queries = result.queries || {};

  const supporting =
    (claimId &&
      typeof queries.whatEvidenceContradictedClaim !== 'function' &&
      collectLinkedEvidence(result, { id: claimId }, 'supporting')) ||
    explanation.supportingEvidence ||
    explanation.supportingSignals ||
    [];

  const contradicting =
    (claimId && typeof queries.whatEvidenceContradictedClaim === 'function'
      ? queries.whatEvidenceContradictedClaim(claimId)
      : null) ||
    explanation.contradictingEvidence ||
    explanation.opposingSignals ||
    [];

  const confidenceHistory = (result.steps || [])
    .map((step) => ({
      at: step.observation && step.observation.observedAt,
      confidence: step.confidence,
      changes: step.confidenceChanges || [],
    }))
    .filter((h) => h.at);

  return Object.freeze({
    entity,
    subject: result.subjectId || null,
    supportingEvidence: Array.isArray(supporting) ? supporting : [],
    contradictingEvidence: Array.isArray(contradicting)
      ? contradicting
      : contradicting
        ? [contradicting]
        : [],
    confidenceHistory,
    reasoningTrace: result.reasoningTrace || null,
    confidence: result.confidence ?? null,
    firstAppeared:
      claimId && typeof queries.whenClaimFirstAppeared === 'function'
        ? queries.whenClaimFirstAppeared(claimId)
        : null,
    becameDominant:
      claimId && typeof queries.whenClaimBecameDominant === 'function'
        ? queries.whenClaimBecameDominant(claimId)
        : null,
  });
}

function normalizeClaims(claims) {
  if (!claims) return [];
  if (Array.isArray(claims)) {
    return claims.filter(Boolean).map(normalizeClaimRow);
  }
  if (typeof claims === 'object') {
    const lists = [
      claims.derived,
      claims.results,
      claims.observations,
      claims.graph,
      claims.items,
      claims.active,
    ];
    return lists.flatMap((list) => (Array.isArray(list) ? list : [])).map(normalizeClaimRow);
  }
  return [];
}

function normalizeClaimRow(claim) {
  if (!claim || typeof claim !== 'object') {
    return { id: String(claim), statement: String(claim) };
  }
  const id = claimIdOf(claim);
  const subject = resolveSubject(claim);
  let confidence = claim.confidence;
  if (confidence != null && Number(confidence) > 1) {
    // keep original; compareValues handles scale
  }
  return {
    ...claim,
    id: id || claim.id,
    subject: subject || claim.subject,
    subjectId: claim.subjectId || subject || null,
    confidence: confidence ?? null,
  };
}

function claimIdOf(claim) {
  if (!claim) return null;
  if (typeof claim === 'string') return claim;
  return (
    claim.id ||
    claim.claimId ||
    claim.claimType ||
    claim.strategy ||
    claim.type ||
    null
  );
}

function normalizeList(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value.filter(Boolean);
  if (typeof value === 'object') {
    if (Array.isArray(value.items)) return value.items.filter(Boolean);
    if (Array.isArray(value.results)) return value.results.filter(Boolean);
    return [value];
  }
  return [];
}

function collectLinkedEvidence(result, claim, kind) {
  const claimId = claimIdOf(claim);
  const explanation = result.explanation || {};
  const key =
    kind === 'supporting'
      ? explanation.supportingEvidence || explanation.supportingSignals || []
      : explanation.contradictingEvidence || explanation.opposingSignals || [];
  const list = Array.isArray(key) ? key : [];
  if (!claimId) return list.slice();
  const filtered = list.filter((item) => {
    if (!item || typeof item !== 'object') return true;
    if (item.claimId && String(item.claimId) === String(claimId)) return true;
    if (Array.isArray(item.claims) && item.claims.map(String).includes(String(claimId))) {
      return true;
    }
    return !item.claimId && !item.claims;
  });
  return filtered.length ? filtered : list.slice();
}

function cloneList(value) {
  return Array.isArray(value) ? value.slice() : [];
}

function stableStringify(value) {
  return JSON.stringify(sortKeys(value));
}

function sortKeys(value) {
  if (Array.isArray(value)) return value.map(sortKeys);
  if (value && typeof value === 'object') {
    const out = {};
    for (const key of Object.keys(value).sort()) {
      out[key] = sortKeys(value[key]);
    }
    return out;
  }
  return value;
}

/**
 * @param {object} [deps]
 * @returns {Executor}
 */
function createExecutor(deps) {
  return new Executor(deps);
}

module.exports = {
  Executor,
  createExecutor,
  createEvidenceCatalog,
  catalogFromResult,
  matchesCondition,
  matchesAll,
  resolveField,
  sortRows,
  rowMatchesId,
};
