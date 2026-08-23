'use strict';

/**
 * SPEC-142 — Evidence Executor.
 * Execute dynamic investigation steps; providers answer questions.
 */

const { evidenceSatisfiesGap, gapsForEvidenceType } = require('./MissingEvidence');

function inferEvidenceType(step, collected = {}) {
  const cap = String(step.capability || '').toLowerCase();
  const provider = String(step.providerId || '').toLowerCase();

  if (provider === 'website' || cap === 'website') return 'website';
  if (provider === 'linkedin' || cap === 'people') return 'linkedin';
  if (provider === 'prospeo' || provider === 'hunter' || cap === 'contacts') return 'contacts';
  if (provider === 'google_maps' || cap === 'businesses') return 'google_maps';
  if (provider === 'news' || cap === 'news') return 'news';
  if (provider === 'county_records') return 'county_records';
  return provider || cap || 'unknown';
}

/**
 * Simulate or execute an investigation step against a candidate.
 * Uses candidate data + optional executeStep hook for live providers.
 *
 * @param {object} step
 * @param {object} candidate
 * @param {object} [opts]
 * @returns {Promise<object>}
 */
async function executeInvestigationStep(step, candidate, opts = {}) {
  if (step.skipped) {
    return {
      step,
      collected: [],
      resolvedGaps: [],
      cost: 0,
      skipped: true,
    };
  }

  if (typeof opts.executeStep === 'function') {
    return opts.executeStep(step, candidate, opts);
  }

  const evidenceType = inferEvidenceType(step);
  const collected = [];
  const resolvedGaps = [];

  if (evidenceType === 'website' && candidate.website) {
    collected.push({
      id: `ev:${candidate.id}:website`,
      source: 'website',
      kind: 'website',
      evidenceType: 'website',
      label: `Website: ${candidate.website}`,
      weight: 0.75,
      observedAt: candidate.updatedAt || new Date().toISOString(),
    });
    if (step.gap === 'cleaning_responsibility' || step.gap === 'business_fit') {
      resolvedGaps.push(step.gap);
    }
  }

  if (evidenceType === 'linkedin' && (candidate.people || []).length > 0) {
    const dm = candidate.people.find((p) => p.jobTitle);
    collected.push({
      id: `ev:${candidate.id}:linkedin`,
      source: 'linkedin',
      kind: 'people',
      evidenceType: 'linkedin',
      label: dm ? `LinkedIn: ${dm.name} (${dm.jobTitle})` : 'LinkedIn company profile',
      weight: 0.82,
      observedAt: candidate.updatedAt || new Date().toISOString(),
    });
    if (step.gap === 'decision_maker') resolvedGaps.push(step.gap);
  }

  if (evidenceType === 'contacts' && (candidate.email || candidate.phone || (candidate.people || []).some((p) => p.email))) {
    collected.push({
      id: `ev:${candidate.id}:contacts`,
      source: 'prospeo',
      kind: 'contacts',
      evidenceType: 'contacts',
      label: `Contact path: ${candidate.email || candidate.people[0].email || candidate.phone}`,
      weight: 0.78,
      observedAt: candidate.updatedAt || new Date().toISOString(),
    });
    if (step.gap === 'contact_path') resolvedGaps.push(step.gap);
  }

  if (evidenceType === 'google_maps') {
    collected.push({
      id: `ev:${candidate.id}:google_maps`,
      source: 'google_maps',
      kind: 'business',
      evidenceType: 'google_maps',
      label: `${candidate.name} listed on Google Maps (${candidate.location || 'unknown'})`,
      weight: 0.9,
      observedAt: candidate.updatedAt || new Date().toISOString(),
    });
    if (step.gap === 'geographic_fit' || step.gap === 'business_fit') resolvedGaps.push(step.gap);
  }

  for (const signal of candidate.signals || []) {
    collected.push({
      id: `ev:${candidate.id}:signal:${signal.type}`,
      source: signal.source || 'news',
      kind: signal.type || 'signal',
      evidenceType: signal.source || 'news',
      label: signal.label || signal.text || signal.type,
      weight: 0.7,
      observedAt: signal.observedAt || new Date().toISOString(),
    });
    if (step.gap === 'buying_signals' || step.gap === 'portfolio_size') resolvedGaps.push(step.gap);
  }

  for (const item of collected) {
    for (const gap of gapsForEvidenceType(item.evidenceType)) {
      if (evidenceSatisfiesGap(gap, [item.evidenceType])) resolvedGaps.push(gap);
    }
  }

  const cost = step.costScore || 1;

  return {
    step,
    collected,
    resolvedGaps: [...new Set(resolvedGaps)],
    cost,
    skipped: false,
  };
}

module.exports = {
  inferEvidenceType,
  executeInvestigationStep,
};
