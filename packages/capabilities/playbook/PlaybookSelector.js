'use strict';

/**
 * Mission Planner → Client Playbook selection (SPEC-028 / ADR-015).
 */

const { buildClientPlaybook } = require('./types');
const { createClientPlaybookStore } = require('./ClientPlaybookStore');

const CLIENT_NAME_HINTS = Object.freeze({
  'as cleaning': 'pb_as_cleaning_co',
  'as cleaning co': 'pb_as_cleaning_co',
  'anchor cleaning': 'pb_anchor_cleaning',
  anchor: 'pb_anchor_cleaning',
});

const CLIENT_ID_HINTS = Object.freeze({
  'as cleaning': 1,
  'as cleaning co': 1,
  'anchor cleaning': 10,
  anchor: 10,
});

/**
 * @param {object} deps
 * @param {import('./ClientPlaybookStore').ClientPlaybookStore} [deps.store]
 */
class PlaybookSelector {
  constructor(deps = {}) {
    this._store = deps.store || createClientPlaybookStore();
  }

  get store() {
    return this._store;
  }

  /**
   * Select a Client Playbook for a mission.
   *
   * @param {object} input
   * @param {string} input.objective
   * @param {string|number} [input.clientId]
   * @param {string|number} [input.tenantId]
   * @param {object} [input.constraints]
   * @returns {{ playbook: object|null, selection: string, alternatives: object[], message: string }}
   */
  select(input = {}) {
    const constraints =
      input.constraints && typeof input.constraints === 'object'
        ? input.constraints
        : {};
    const objective = String(input.objective || '');

    if (constraints.clientPlaybook && typeof constraints.clientPlaybook === 'object') {
      const playbook = buildClientPlaybook(constraints.clientPlaybook);
      return {
        playbook,
        selection: 'explicit',
        alternatives: [],
        message: `Using Client Playbook: ${playbook.name} (v${playbook.version}).`,
      };
    }

    if (constraints.clientPlaybookId) {
      const playbook = this._store.get(
        constraints.clientPlaybookId,
        constraints.clientPlaybookVersion
      );
      if (playbook) {
        return {
          playbook: this._store.snapshot(playbook),
          selection: 'pinned',
          alternatives: [],
          message: `Using Client Playbook: ${playbook.name} (v${playbook.version}).`,
        };
      }
    }

    const hintedId = inferPlaybookId(objective);
    if (hintedId) {
      const playbook = this._store.get(hintedId);
      if (playbook) {
        return {
          playbook: this._store.snapshot(playbook),
          selection: 'objective_hint',
          alternatives: [],
          message: `Using Client Playbook: ${playbook.name} (v${playbook.version}).`,
        };
      }
    }

    const clientId =
      input.clientId != null
        ? input.clientId
        : inferClientId(objective) != null
          ? inferClientId(objective)
          : input.tenantId;

    const candidates = this._store.list({
      clientId,
      status: 'active',
    });

    if (candidates.length === 1) {
      const playbook = this._store.snapshot(candidates[0]);
      return {
        playbook,
        selection: 'client',
        alternatives: [],
        message: `Using Client Playbook: ${playbook.name} (v${playbook.version}).`,
      };
    }

    if (candidates.length > 1) {
      const playbook = this._store.snapshot(candidates[0]);
      return {
        playbook,
        selection: 'client_ambiguous',
        alternatives: candidates.slice(1).map((p) => this._store.snapshot(p)),
        message: `Using Client Playbook: ${playbook.name} (v${playbook.version}). ${candidates.length - 1} alternative(s) available.`,
      };
    }

    // Fall back to any active playbook matching objective keywords
    const all = this._store.list({ status: 'active' });
    const scored = all
      .map((p) => ({ playbook: p, score: scorePlaybookMatch(p, objective) }))
      .filter((x) => x.score > 0)
      .sort((a, b) => b.score - a.score);

    if (scored.length) {
      const playbook = this._store.snapshot(scored[0].playbook);
      return {
        playbook,
        selection: 'scored',
        alternatives: scored.slice(1, 4).map((x) => this._store.snapshot(x.playbook)),
        message: `Using Client Playbook: ${playbook.name} (v${playbook.version}).`,
      };
    }

    return {
      playbook: null,
      selection: 'none',
      alternatives: [],
      message:
        'No Client Playbook found — Campaign Builder and Proposal Generator will operate without playbook strategy until one is created.',
    };
  }
}

function inferPlaybookId(objective) {
  const lower = String(objective || '').toLowerCase();
  for (const [hint, id] of Object.entries(CLIENT_NAME_HINTS)) {
    if (lower.includes(hint)) return id;
  }
  return null;
}

function inferClientId(objective) {
  const lower = String(objective || '').toLowerCase();
  for (const [hint, id] of Object.entries(CLIENT_ID_HINTS)) {
    if (lower.includes(hint)) return id;
  }
  return null;
}

function scorePlaybookMatch(playbook, objective) {
  const lower = String(objective || '').toLowerCase();
  let score = 0;
  if (playbook.name && lower.includes(String(playbook.name).toLowerCase().slice(0, 12))) {
    score += 5;
  }
  for (const market of playbook.targetMarkets || []) {
    if (lower.includes(String(market).toLowerCase())) score += 2;
  }
  if (playbook.idealCustomer && playbook.idealCustomer.geographicCoverage) {
    const geo = String(playbook.idealCustomer.geographicCoverage).toLowerCase();
    if (geo.includes('toronto') && lower.includes('toronto')) score += 3;
    if (geo.includes('manchester') && lower.includes('manchester')) score += 3;
  }
  return score;
}

function createPlaybookSelector(deps) {
  return new PlaybookSelector(deps);
}

module.exports = {
  PlaybookSelector,
  createPlaybookSelector,
  inferPlaybookId,
  inferClientId,
};
