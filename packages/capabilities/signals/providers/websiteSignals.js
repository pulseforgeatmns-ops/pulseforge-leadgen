'use strict';

/**
 * Marketing / website collectors (SPEC-031).
 * Emit only when website freshness / rebrand evidence is present.
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
function collectWebsiteSignals(prospect, ctx = {}) {
  const out = [];
  const knowledge = ctx.knowledge || {};
  const evidence = [];

  if (
    prospect.websiteFresh === true ||
    prospect.newWebsite === true ||
    knowledge.websiteFresh === true ||
    knowledge.newWebsite === true
  ) {
    evidence.push(
      buildSignalEvidence({
        kind: 'enrichment_flag',
        summary: 'New/fresh website flag evidenced',
        rawRef: 'enrichment:new_website',
        observedAt:
          prospect.websiteUpdatedAt || prospect.observedAt || ctx.asOf,
      })
    );
  }

  if (prospect.websiteUpdatedAt || knowledge.websiteUpdatedAt) {
    const updatedAt = prospect.websiteUpdatedAt || knowledge.websiteUpdatedAt;
    const updated = new Date(updatedAt);
    const asOf = ctx.asOf ? new Date(ctx.asOf) : new Date();
    const ageDays =
      (asOf.getTime() - updated.getTime()) / (24 * 60 * 60 * 1000);
    if (!Number.isNaN(ageDays) && ageDays >= 0 && ageDays <= 90) {
      evidence.push(
        buildSignalEvidence({
          kind: 'website_timestamp',
          summary: `Website updatedAt within 90 days (${updated.toISOString()})`,
          rawRef: `website:updatedAt:${updated.toISOString()}`,
          observedAt: updated.toISOString(),
          url: prospect.website || knowledge.website,
        })
      );
    }
  }

  const evidenceItems = [
    ...(Array.isArray(prospect.evidence) ? prospect.evidence : []),
    ...(Array.isArray(knowledge.evidence) ? knowledge.evidence : []),
  ];
  for (const item of evidenceItems) {
    const summary = String(item.summary || item.detail || item.text || '');
    const kind = String(item.kind || item.type || '').toLowerCase();
    if (
      kind.includes('website') ||
      kind.includes('rebrand') ||
      /new\s+website|redesign|rebrand|launched\s+(a\s+)?site/i.test(summary)
    ) {
      evidence.push(
        buildSignalEvidence({
          kind: kind || 'website_observation',
          summary: summary || 'Website/marketing evidence',
          url: item.url || prospect.website,
          rawRef: item.rawRef || item.id || `evidence:website:${evidence.length}`,
          observedAt: item.observedAt || prospect.observedAt || ctx.asOf,
        })
      );
    }
  }

  // Soft website URL alone is NOT a signal — need freshness/rebrand evidence
  if (evidence.length === 0) return out;

  const rebrand = evidence.some((e) => /rebrand/i.test(e.summary + e.kind));
  const type = rebrand ? 'recent_rebrand' : 'new_website';
  const confidence = deriveConfidence({
    evidenceCount: evidence.length,
    sourceCount: evidence.length,
    indirect: evidence.length === 1,
  });

  out.push(
    buildBusinessSignal({
      id: signalId(prospect, type),
      type,
      category: SIGNAL_CATEGORY.MARKETING,
      title: rebrand ? 'Recent Rebrand' : 'New Website',
      description: rebrand
        ? 'Recent rebrand evidenced — brand and vendor relationships may be in motion.'
        : 'Website appears newly launched or recently updated — presentation and vendor mix may be changing.',
      confidence,
      confidenceScore: scoreForConfidence(confidence),
      lifecycle: SIGNAL_LIFECYCLE.DETECTED,
      observedAt: iso(
        prospect.websiteUpdatedAt || prospect.observedAt || ctx.asOf
      ),
      source: 'website_collector',
      evidence,
      evidenceRefs: evidence.map((e) => e.rawRef).filter(Boolean),
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
  collectWebsiteSignals,
};
