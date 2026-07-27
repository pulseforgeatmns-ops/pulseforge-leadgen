'use strict';

/**
 * Business Signals builder (SPEC-031 / ADR-018).
 *
 * Company → Collect → Verify → Active set → Ranking / Brief / Campaign / Knowledge
 * Never fabricates observations.
 */

const { collectSignals } = require('./collect');
const { verifySignals } = require('./verify');
const { buildKnowledgeWrites } = require('./knowledgeHandoff');
const { buildSignalsPackage, SIGNAL_LIFECYCLE } = require('./types');
const {
  selectActive,
  messagingPostureFromSignals,
  activeSignalsForOperator,
  buyingSignalsForRanking,
} = require('./messaging');

/**
 * Build a full signals package for one prospect.
 *
 * @param {object} prospect
 * @param {object} [ctx]
 * @param {object} [ctx.knowledge]
 * @param {object} [ctx.playbook] - optional; preferredSignals enable Low→Active
 * @param {Date|string|number} [ctx.asOf]
 * @returns {object} SignalsPackage + operator/messaging helpers
 */
function buildBusinessSignalsForProspect(prospect, ctx = {}) {
  const preferredSignalTypes = resolvePreferredTypes(ctx);
  const asOf = ctx.asOf || new Date().toISOString();
  const knowledge = resolveKnowledge(prospect, ctx.knowledge);

  const detected = collectSignals(prospect, { ...ctx, knowledge, asOf });
  const signals = verifySignals(detected, {
    preferredSignalTypes,
    asOf,
  });

  const activeSignals = selectActive(signals);
  const buyingSignals = buyingSignalsForRanking(signals);
  const archivedCount = signals.filter(
    (s) => s.lifecycle === SIGNAL_LIFECYCLE.ARCHIVED
  ).length;
  const knowledgeWrites = buildKnowledgeWrites(signals);
  const messaging = messagingPostureFromSignals(signals);

  const pkg = buildSignalsPackage({
    signals,
    activeSignals,
    buyingSignals,
    archivedCount,
    knowledgeWrites,
  });

  return {
    ...pkg,
    operatorSignals: activeSignalsForOperator(signals),
    messagingPosture: messaging.posture,
    messagingDescription: messaging.description,
    drivingSignal: messaging.drivingSignal,
  };
}

/**
 * Build packages for many prospects (Company Intelligence Signals stage hook).
 * @param {object[]} prospects
 * @param {object} [ctx]
 * @returns {{ packages: object[], prospects: object[], knowledgeWrites: object[] }}
 */
function buildBusinessSignalsStage(prospects, ctx = {}) {
  const list = Array.isArray(prospects) ? prospects : [];
  const packages = [];
  const knowledgeWrites = [];
  const enriched = list.map((p) => {
    const pkg = buildBusinessSignalsForProspect(p, ctx);
    packages.push({ prospectId: p.id, companyName: p.companyName, ...pkg });
    knowledgeWrites.push(...pkg.knowledgeWrites);
    return {
      ...p,
      businessSignals: pkg.signals,
      activeSignals: pkg.activeSignals,
      buyingSignals: pkg.buyingSignals,
      operatorSignals: pkg.operatorSignals,
      messagingPosture: pkg.messagingPosture,
      messagingDescription: pkg.messagingDescription,
    };
  });

  return {
    packages,
    prospects: enriched,
    knowledgeWrites,
    activeCount: packages.reduce((n, p) => n + p.activeSignals.length, 0),
  };
}

/**
 * Resolve Active signals already on a prospect (for Ranking/Brief without rebuild).
 * Falls back to building from evidence when structured signals absent.
 *
 * @param {object} prospect
 * @param {object} [knowledge]
 * @param {object} [opts]
 * @returns {object[]}
 */
function resolveActiveSignals(prospect, knowledge = {}, opts = {}) {
  if (Array.isArray(prospect.activeSignals) && prospect.activeSignals.length) {
    return selectActive(prospect.activeSignals);
  }
  if (Array.isArray(prospect.businessSignals) && prospect.businessSignals.length) {
    return selectActive(prospect.businessSignals);
  }
  if (Array.isArray(knowledge.activeSignals) && knowledge.activeSignals.length) {
    return selectActive(knowledge.activeSignals);
  }
  if (Array.isArray(knowledge.businessSignals) && knowledge.businessSignals.length) {
    return selectActive(knowledge.businessSignals);
  }
  // Lazy build from evidenced fields when Ranking sees enrichment flags only
  const pkg = buildBusinessSignalsForProspect(prospect, {
    knowledge,
    asOf: opts.asOf,
    playbook: opts.playbook,
  });
  return pkg.activeSignals;
}

function resolvePreferredTypes(ctx) {
  const playbook = ctx.playbook || null;
  const profile = ctx.profile || null;
  const types = [];
  if (playbook && Array.isArray(playbook.preferredSignals)) {
    types.push(...playbook.preferredSignals);
  }
  if (profile && Array.isArray(profile.preferredSignals)) {
    types.push(...profile.preferredSignals);
  }
  if (Array.isArray(ctx.preferredSignalTypes)) {
    types.push(...ctx.preferredSignalTypes);
  }
  return types.map(String);
}

function resolveKnowledge(prospect, knowledgeRoot) {
  if (!knowledgeRoot || typeof knowledgeRoot !== 'object') return {};
  const id = prospect.id || prospect.companyId || prospect.companyName;
  if (knowledgeRoot.byProspectId && id && knowledgeRoot.byProspectId[id]) {
    return knowledgeRoot.byProspectId[id];
  }
  if (knowledgeRoot.byCompanyName && prospect.companyName) {
    const hit = knowledgeRoot.byCompanyName[prospect.companyName];
    if (hit) return hit;
  }
  if (
    knowledgeRoot.hiringActivity != null ||
    knowledgeRoot.buyingSignals ||
    knowledgeRoot.businessSignals ||
    knowledgeRoot.evidence
  ) {
    return knowledgeRoot;
  }
  return {};
}

module.exports = {
  buildBusinessSignalsForProspect,
  buildBusinessSignalsStage,
  resolveActiveSignals,
};
