'use strict';

/**
 * Multi-location / commercial footprint collectors (SPEC-031).
 * Emit only when discovery rankingSignals or firmographic fields already evidence the fact.
 */

const {
  SIGNAL_CATEGORY,
  SIGNAL_LIFECYCLE,
  buildBusinessSignal,
  buildSignalEvidence,
} = require('../types');
const { deriveConfidence, scoreForConfidence } = require('../confidence');

/**
 * @param {object} prospect
 * @param {object} [ctx]
 * @returns {object[]}
 */
function collectMultiLocation(prospect, ctx = {}) {
  const out = [];
  const rankingSignals = Array.isArray(prospect.rankingSignals)
    ? prospect.rankingSignals
    : [];
  const matched = rankingSignals.find(
    (s) => s.signal === 'multi_location' && s.matched
  );
  const flag =
    prospect.multiLocation === true ||
    prospect.multi_location === true ||
    (ctx.knowledge && ctx.knowledge.multiLocation === true);

  if (!matched && !flag) return out;

  const evidence = [];
  if (matched) {
    evidence.push(
      buildSignalEvidence({
        kind: 'ranking_signal',
        summary: matched.detail || 'Matched multi_location discovery signal',
        rawRef: 'ranking_signal:multi_location',
        observedAt: prospect.observedAt || ctx.asOf,
      })
    );
  }
  if (flag) {
    evidence.push(
      buildSignalEvidence({
        kind: 'firmographic',
        summary: 'Multi-location flag evidenced on prospect/knowledge',
        rawRef: 'firmographic:multi_location',
        observedAt: prospect.observedAt || ctx.asOf,
      })
    );
  }

  const confidence = deriveConfidence({
    evidenceCount: evidence.length,
    sourceCount: evidence.length,
    indirect: evidence.length === 1,
  });

  out.push(
    buildBusinessSignal({
      id: signalId(prospect, 'multi_location'),
      type: 'multi_location',
      category: SIGNAL_CATEGORY.OPERATIONAL,
      title: 'Multiple Locations',
      description:
        'Operates across more than one location — facility demand may span sites.',
      confidence,
      confidenceScore: scoreForConfidence(confidence),
      lifecycle: SIGNAL_LIFECYCLE.DETECTED,
      observedAt: iso(prospect.observedAt || ctx.asOf),
      source: matched ? 'discovery.rankingSignals' : 'firmographics',
      evidence,
      evidenceRefs: evidence.map((e) => e.rawRef),
      prospectId: prospect.id,
      companyId: prospect.companyId,
    })
  );
  return out;
}

/**
 * @param {object} prospect
 * @param {object} [ctx]
 * @returns {object[]}
 */
function collectCommercialFootprint(prospect, ctx = {}) {
  const out = [];
  const rankingSignals = Array.isArray(prospect.rankingSignals)
    ? prospect.rankingSignals
    : [];
  const matched = rankingSignals.find(
    (s) =>
      (s.signal === 'commercial_office' || s.signal === 'commercial_footprint') &&
      s.matched
  );
  if (!matched) return out;

  const evidence = [
    buildSignalEvidence({
      kind: 'ranking_signal',
      summary: matched.detail || `Matched ${matched.signal} discovery signal`,
      rawRef: `ranking_signal:${matched.signal}`,
      observedAt: prospect.observedAt || ctx.asOf,
    }),
  ];
  const confidence = deriveConfidence({
    evidenceCount: 1,
    sourceCount: 1,
    indirect: true,
  });

  out.push(
    buildBusinessSignal({
      id: signalId(prospect, 'commercial_footprint'),
      type: 'commercial_footprint',
      category: SIGNAL_CATEGORY.OPERATIONAL,
      title: 'Commercial Office Footprint',
      description:
        'Evidenced commercial office footprint — recurring facility cleaning demand is plausible.',
      confidence,
      confidenceScore: scoreForConfidence(confidence),
      lifecycle: SIGNAL_LIFECYCLE.DETECTED,
      observedAt: iso(prospect.observedAt || ctx.asOf),
      source: 'discovery.rankingSignals',
      evidence,
      evidenceRefs: evidence.map((e) => e.rawRef),
      prospectId: prospect.id,
      companyId: prospect.companyId,
    })
  );
  return out;
}

function signalId(prospect, type) {
  const base = prospect.id || prospect.companyName || 'unknown';
  return `sig_${base}_${type}`.replace(/\s+/g, '_').toLowerCase();
}

function iso(value) {
  if (!value) return new Date().toISOString();
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? new Date().toISOString() : d.toISOString();
}

module.exports = {
  collectMultiLocation,
  collectCommercialFootprint,
};
