'use strict';

/**
 * Hiring / expansion collectors (SPEC-031).
 * Never invent — only emit when hiring/expansion evidence is already present.
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
function collectHiring(prospect, ctx = {}) {
  const out = [];
  const knowledge = ctx.knowledge || {};
  const evidence = [];

  if (prospect.hiringActivity === true || knowledge.hiringActivity === true) {
    evidence.push(
      buildSignalEvidence({
        kind: 'enrichment_flag',
        summary: 'Hiring activity flag evidenced on prospect/knowledge',
        rawRef: 'enrichment:hiring_activity',
        observedAt: prospect.hiringObservedAt || prospect.observedAt || ctx.asOf,
      })
    );
  }

  const evidenceItems = [
    ...(Array.isArray(prospect.evidence) ? prospect.evidence : []),
    ...(Array.isArray(knowledge.evidence) ? knowledge.evidence : []),
  ];
  for (const item of evidenceItems) {
    const kind = String(item.kind || item.type || '').toLowerCase();
    const summary = String(item.summary || item.detail || item.text || '');
    if (
      kind.includes('hir') ||
      /hiring|careers|job\s*post|open\s*role|we're hiring|we are hiring/i.test(
        summary
      )
    ) {
      evidence.push(
        buildSignalEvidence({
          kind: kind || 'hiring_observation',
          summary: summary || 'Hiring-related evidence item',
          url: item.url,
          rawRef: item.rawRef || item.id || `evidence:hiring:${evidence.length}`,
          observedAt: item.observedAt || prospect.observedAt || ctx.asOf,
        })
      );
    }
  }

  if (
    Array.isArray(prospect.jobPostings) &&
    prospect.jobPostings.length > 0
  ) {
    evidence.push(
      buildSignalEvidence({
        kind: 'job_posting',
        summary: `${prospect.jobPostings.length} job posting(s) evidenced`,
        rawRef: 'job_postings',
        observedAt: prospect.observedAt || ctx.asOf,
      })
    );
  }

  if (evidence.length === 0) return out;

  const facilitiesHire = evidence.some((e) =>
    /facilit|office\s*manager|admin|operations/i.test(e.summary)
  );
  const type = facilitiesHire ? 'hiring_facilities' : 'hiring_office_staff';
  const confidence = deriveConfidence({
    evidenceCount: evidence.length,
    sourceCount: evidence.length,
    official: evidence.some((e) => e.kind === 'job_posting'),
    indirect: evidence.length === 1 && evidence[0].kind === 'enrichment_flag',
  });

  out.push(
    buildBusinessSignal({
      id: signalId(prospect, type),
      type,
      category:
        type === 'hiring_facilities'
          ? SIGNAL_CATEGORY.BUYING
          : SIGNAL_CATEGORY.GROWTH,
      title: facilitiesHire ? 'Hiring Facilities Personnel' : 'Hiring Office Staff',
      description: facilitiesHire
        ? 'Hiring related to facilities/admin — timing may favor a cleaning conversation.'
        : 'Appears to be hiring administrative or office staff — operational load may be rising.',
      confidence,
      confidenceScore: scoreForConfidence(confidence),
      lifecycle: SIGNAL_LIFECYCLE.DETECTED,
      observedAt: iso(
        prospect.hiringObservedAt || prospect.observedAt || ctx.asOf
      ),
      source: 'hiring_collector',
      evidence,
      evidenceRefs: evidence.map((e) => e.rawRef).filter(Boolean),
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
function collectExpansion(prospect, ctx = {}) {
  const out = [];
  const knowledge = ctx.knowledge || {};
  const evidence = [];

  const expansionKeys = [
    ['expanding', prospect.expanding || knowledge.expanding],
    ['newLocation', prospect.newLocation || knowledge.newLocation],
    ['new_location', prospect.new_location || knowledge.new_location],
    [
      'officeExpansion',
      prospect.officeExpansion || knowledge.officeExpansion,
    ],
  ];

  for (const [key, value] of expansionKeys) {
    if (value === true || value === 1 || value === 'true') {
      evidence.push(
        buildSignalEvidence({
          kind: 'enrichment_flag',
          summary: `Expansion flag evidenced: ${key}`,
          rawRef: `enrichment:${key}`,
          observedAt: prospect.observedAt || ctx.asOf,
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
      kind.includes('expans') ||
      kind.includes('location') ||
      /new\s+office|second\s+office|satellite|expand(ed|ing)|opened\s+a\s+(new\s+)?location/i.test(
        summary
      )
    ) {
      evidence.push(
        buildSignalEvidence({
          kind: kind || 'expansion_observation',
          summary: summary || 'Expansion-related evidence',
          url: item.url,
          rawRef: item.rawRef || item.id || `evidence:expansion:${evidence.length}`,
          observedAt: item.observedAt || prospect.observedAt || ctx.asOf,
        })
      );
    }
  }

  if (evidence.length === 0) return out;

  const confidence = deriveConfidence({
    evidenceCount: evidence.length,
    sourceCount: evidence.length,
    official: evidence.some((e) => /announcement|press|official/i.test(e.kind)),
    indirect: evidence.length === 1,
  });

  out.push(
    buildBusinessSignal({
      id: signalId(prospect, 'new_location'),
      type: 'new_location',
      category: SIGNAL_CATEGORY.BUYING,
      title: 'Recently Expanded',
      description:
        'Evidence of expansion or a new location — facility coverage and cleaning capacity may be in flux.',
      confidence,
      confidenceScore: scoreForConfidence(confidence),
      lifecycle: SIGNAL_LIFECYCLE.DETECTED,
      observedAt: iso(prospect.observedAt || ctx.asOf),
      source: 'expansion_collector',
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
  collectHiring,
  collectExpansion,
};
