'use strict';

/**
 * SPEC-133 — Normalize Scout intelligence into AMO discovery contribution payloads.
 * Preserves evidence provenance, signal specificity, and confidence decomposition.
 */

const { buildIntelligenceBrief } = require('../scout/credibility/CredibilityFramework');
const {
  buildPublicMissionIntelligenceReport,
  containsForbiddenReasoningKeys,
} = require('../scout/investigation/MissionIntelligenceReport');
const {
  buildScoutDiscoveryArtifact,
  mapCanonicalEvidenceToContribution,
} = require('../scout/adapters/ScoutDiscoveryArtifact');
const { normalizeProviderExecution } = require('../scout/coverage/ProviderExecution');
const { READINESS_STATES } = require('../max/scoutAcquisition/Types');

const SOURCE_LABELS = Object.freeze({
  existing_repository: 'Company repository',
  google_places: 'Google Places',
  linkedin: 'LinkedIn',
  website: 'Company website',
  news: 'News',
  job_board: 'Job board',
  apollo: 'Apollo',
  fixture: 'Test fixture (not live data)',
});

function formatDiscoveryItem(item) {
  if (item == null) return '';
  if (typeof item === 'string') return item.trim();
  if (typeof item === 'number' || typeof item === 'boolean') return String(item);
  if (typeof item === 'object') {
    if (item.name) return String(item.name);
    if (item.label) return String(item.label);
    if (item.signal) return String(item.signal);
  }
  return String(item);
}

function sourceLabel(source) {
  const key = String(source || '').toLowerCase().replace(/[\s-]+/g, '_');
  return SOURCE_LABELS[key] || (source ? String(source) : 'Unknown source');
}

function formatSignalLabel(signal) {
  if (!signal || typeof signal !== 'object') return formatDiscoveryItem(signal);
  if (signal.label) return String(signal.label);
  const type = String(signal.type || signal.kind || 'signal').replace(/_/g, ' ');
  const role = signal.role ? ` (${signal.role})` : '';
  return `${type.charAt(0).toUpperCase()}${type.slice(1)}${role}`;
}

function normalizeBuyingSignal(raw, companyName) {
  if (raw == null) return null;
  if (typeof raw === 'string') {
    const text = raw.trim();
    if (!text) return null;
    return { label: text, type: 'signal', company: companyName || null, source: null };
  }
  if (typeof raw !== 'object') return null;
  return {
    type: raw.type || raw.kind || 'signal',
    label: formatSignalLabel(raw),
    company: companyName || raw.company || null,
    source: sourceLabel(raw.source),
    observedAt: raw.observedAt || raw.observed_at || null,
  };
}

function normalizeEvidenceItem(raw, companyName) {
  if (raw == null) return null;
  if (typeof raw === 'string') {
    const text = raw.trim();
    if (!text) return null;
    return {
      label: text,
      source: text.toLowerCase() === 'fixture' ? 'Test fixture (not live data)' : text,
      company: companyName || null,
    };
  }
  if (typeof raw !== 'object') return null;
  const snapshot = raw.snapshot && typeof raw.snapshot === 'object' ? raw.snapshot : {};
  return {
    label: raw.label || snapshot.companyName || formatDiscoveryItem(raw),
    source: sourceLabel(snapshot.source || raw.sourceKind || raw.source),
    company: companyName || snapshot.companyName || null,
    observedAt: snapshot.observedAt || raw.observedAt || null,
    evidenceType: snapshot.evidenceType || raw.kind || null,
  };
}

function buildProspectRationale(opp) {
  const parts = [];
  if (opp.fit != null) parts.push(`fit ${Number(opp.fit).toFixed(2)}`);
  if (opp.timing != null) parts.push(`timing ${Number(opp.timing).toFixed(2)}`);
  const signals = Array.isArray(opp.signals) ? opp.signals : [];
  if (signals.length) {
    const signalLabels = signals.slice(0, 3).map((s) => formatSignalLabel(s)).join('; ');
    parts.push(`signals: ${signalLabels}`);
  }
  const unknowns = Array.isArray(opp.unknowns) ? opp.unknowns : [];
  if (unknowns.length) {
    parts.push(`${unknowns.length} unknown${unknowns.length === 1 ? '' : 's'}`);
  }
  return parts.length ? parts.join(' · ') : 'Matches mission objective under current criteria.';
}

function buildOpportunityCredibilityBrief(opp, index) {
  const evidence = (opp.evidenceRefs || []).map((ref) => normalizeEvidenceItem(ref, opp.name)).filter(Boolean);
  const supportedBy = evidence.map((e) => ({
    source: e.source,
    label: e.label,
    observedAt: e.observedAt,
  }));

  return buildIntelligenceBrief({
    rankingEntry: {
      rank: index + 1,
      name: opp.name,
      companyId: opp.companyId || opp.id,
      rankScore: opp.fit != null ? Number(opp.fit) : null,
      evidenceConfidence: opp.confidence != null ? Number(opp.confidence) : null,
      reasons: [buildProspectRationale(opp)],
      signals: opp.signals || [],
      scores: {
        revenue_potential: opp.fit != null ? Number(opp.fit) * 0.2 : 0,
        buying_signals: (opp.signals || []).length ? 0.18 : 0.05,
        evidence_confidence: supportedBy.length ? 0.11 : 0.03,
        geographic_fit: 0.15,
        relationship_probability: 0.12,
        ease_of_access: 0.14,
        strategic_value: 0.08,
      },
    },
    candidate: {
      id: opp.companyId || opp.id,
      name: opp.name,
      signals: opp.signals || [],
      evidence: opp.evidenceRefs || [],
    },
    claims: supportedBy.length
      ? [
          {
            entityId: opp.companyId || opp.id,
            text: `${opp.name} matches mission objective under current Scout evidence.`,
            confidence: opp.confidence != null ? Number(opp.confidence) : 0.5,
            supportedBy,
            missingEvidence: (opp.unknowns || []).map((u) => (typeof u === 'object' ? u.text : String(u))),
          },
        ]
      : [],
    missingEvidence: (opp.unknowns || []).map((u) => (typeof u === 'object' ? u.text : String(u))),
  });
}

function computeConfidenceBreakdown(opportunities, payload, rollupConfidence) {
  const count = opportunities.length;
  const withSignals = opportunities.filter((o) => (o.signals || []).length > 0).length;
  const withEvidence = opportunities.filter((o) => (o.evidenceRefs || []).length > 0).length;
  const withDm = opportunities.filter((o) =>
    (o.signals || []).some((s) => s.type === 'decision_maker')
  ).length;
  const timely = opportunities.filter((o) =>
    (o.signals || []).some((s) => s.observedAt)
  ).length;

  const discovery = count > 0 ? Math.min(0.95, 0.4 + count * 0.08) : 0.15;
  const evidence = count > 0 ? withEvidence / count : 0;
  const market = payload.searchDefinitionValid === false ? 0.25 : 0.65;
  const fit =
    count > 0
      ? opportunities.reduce((sum, o) => sum + (Number(o.fit) || 0.5), 0) / count
      : 0.2;
  const completeness =
    count > 0
      ? (withSignals + withDm + withEvidence) / (count * 3)
      : 0;

  const overall =
    rollupConfidence != null
      ? Number(rollupConfidence)
      : Number(
          (
            discovery * 0.2 +
            evidence * 0.25 +
            market * 0.15 +
            fit * 0.25 +
            completeness * 0.15
          ).toFixed(2)
        );

  return {
    overall: Number(overall.toFixed(2)),
    discovery: Number(discovery.toFixed(2)),
    evidence: Number(evidence.toFixed(2)),
    market: Number(market.toFixed(2)),
    fit: Number(fit.toFixed(2)),
    completeness: Number(completeness.toFixed(2)),
    signalBearing: withSignals,
    timelySignals: timely,
    missingEvidence: count > 0 && withEvidence < count
      ? [`${count - withEvidence} prospect(s) lack attributable evidence`]
      : [],
    unknowns: opportunities.flatMap((o) =>
      (o.unknowns || []).map((u) => (typeof u === 'object' ? u.text : String(u))).filter(Boolean)
    ).slice(0, 5),
  };
}

function buildDiscoverySummary(opportunities, missionObjective) {
  if (!opportunities.length) {
    return missionObjective
      ? `No prospects matched the mission objective: ${missionObjective}`
      : 'No prospects matched the mission objective under current criteria.';
  }
  const names = opportunities.slice(0, 3).map((o) => o.name).filter(Boolean);
  const withDm = opportunities.filter((o) =>
    (o.signals || []).some((s) => s.type === 'decision_maker')
  ).length;
  const parts = [
    `${opportunities.length} prospect${opportunities.length === 1 ? '' : 's'} ranked against the mission objective.`,
  ];
  if (names.length) parts.push(`Top: ${names.join(', ')}.`);
  if (withDm) parts.push(`${withDm} have identifiable decision-makers.`);
  return parts.join(' ');
}

/**
 * Project candidate universe records for AMO contribution boundary (SPEC-173 / SPEC-196).
 * Merges hypotheses into unknowns and strips contract-forbidden keys.
 * @param {object[]} records
 * @returns {object[]}
 */
function projectCandidateUniverseForContribution(records = []) {
  if (!Array.isArray(records)) return [];
  return records.map((record = {}) => {
    const unknowns = [
      ...(Array.isArray(record.unknowns) ? record.unknowns : []),
      ...(Array.isArray(record.hypotheses) ? record.hypotheses : []),
    ];
    const investigationState =
      record.investigationState && typeof record.investigationState === 'object'
        ? {
            missingEvidence: record.investigationState.missingEvidence || [],
            unresolvedHypotheses: record.investigationState.unresolvedHypotheses || [],
            canonicalGaps: record.investigationState.canonicalGaps || [],
          }
        : null;

    const projected = {
      candidateId: record.candidateId || record.candidate_id || record.id || null,
      candidate_id: record.candidate_id || record.candidateId || record.id || null,
      canonicalIdentity:
        record.canonicalIdentity || record.candidateId || record.candidate_id || record.id || null,
      name: record.name || null,
      placeId: record.placeId || record.place_id || null,
      website: record.website || record.url || null,
      phone: record.phone || null,
      address: record.address || record.location || null,
      evidence: record.evidence || null,
      qualification: record.qualification || null,
      readiness: record.readiness || null,
      origin: record.origin || null,
      sources: record.sources || null,
      cities: record.cities || null,
      confidence: record.confidence != null ? Number(record.confidence) : null,
      dedupeStatus: record.dedupeStatus || null,
      concept: record.concept || null,
    };

    if (unknowns.length) projected.unknowns = unknowns;
    if (investigationState) projected.investigationState = investigationState;
    if (record.excluded === true) projected.excluded = true;

    return projected;
  });
}

/**
 * @param {object} result - Scout intelligence result
 * @param {object} [opts]
 * @returns {object}
 */
function normalizeScoutDiscoveryPayload(result = {}, opts = {}) {
  const artifact =
    opts.discoveryArtifact ||
    buildScoutDiscoveryArtifact(result, {
      missionObjective: opts.missionObjective,
      approvalConsumed: opts.approvalConsumed,
    });
  const payload = artifact.sourceResult?.payload || result.payload || {};
  const opportunities = artifact.opportunities || [];
  const fitCandidates = artifact.fitCandidates || payload.fitCandidates || [];
  const watchCandidates = artifact.watchCandidates || payload.watchCandidates || [];
  const missionObjective = artifact.missionObjective || opts.missionObjective || payload.missionObjective || null;

  const companies = artifact.companies || [];
  const prospects = artifact.prospects || [];
  const qualifiedFromEvaluations =
    payload.readinessUnknownCount != null || payload.readinessReadyCount != null
      ? Number(payload.readinessReadyCount || 0) +
        Number(payload.readinessUnknownCount || 0) +
        Number(payload.readinessNotReadyCount || 0)
      : null;
  const qualifiedCount =
    artifact.qualifiedCount != null
      ? Number(artifact.qualifiedCount)
      : qualifiedFromEvaluations != null && qualifiedFromEvaluations > 0
        ? qualifiedFromEvaluations
        : opportunities.length +
            fitCandidates.length +
            watchCandidates.filter((row) => row.qualified === true).length || companies.length;

  const buyingSignals = [];
  const seenSignals = new Set();
  for (const sig of artifact.buyingSignals || []) {
    const normalized = normalizeBuyingSignal(sig, sig.company);
    if (!normalized) continue;
    const key = `${normalized.company || ''}|${normalized.label}`;
    if (seenSignals.has(key)) continue;
    seenSignals.add(key);
    buyingSignals.push(normalized);
  }

  const evidence = mapCanonicalEvidenceToContribution(artifact.evidence || []).map((item) =>
    normalizeEvidenceItem(
      {
        id: item.id,
        label: item.label,
        source: item.source,
        observedAt: item.observedAt,
        kind: item.evidenceType,
        provenance: item.provenance,
        snapshot: {
          source: item.source,
          companyName: item.company,
          evidenceType: item.evidenceType,
          observedAt: item.observedAt,
        },
      },
      item.company
    )
  ).filter(Boolean);

  const decisionMakers = [];
  for (const opp of opportunities) {
    const dmSignal = (opp.signals || []).find((s) => s.type === 'decision_maker');
    if (dmSignal && dmSignal.label) {
      decisionMakers.push({ name: dmSignal.label, company: opp.name });
    }
  }
  for (const dm of payload.decisionMakers || []) {
    if (typeof dm === 'string') decisionMakers.push({ name: dm });
    else if (dm && dm.name) decisionMakers.push(dm);
  }

  function buildRankedProspectRow(opp, index, readinessState) {
    const credibilityBrief = buildOpportunityCredibilityBrief(opp, index);
    const evaluation = opp.evaluation || null;
    return {
      rank: index + 1,
      name: opp.name,
      id: opp.companyId || opp.id || null,
      fit: opp.fit != null ? Number(opp.fit) : null,
      timing: opp.timing != null ? Number(opp.timing) : null,
      confidence: opp.confidence != null ? Number(opp.confidence) : null,
      readinessState,
      qualificationStatus: evaluation && evaluation.qualification ? evaluation.qualification.status : opp.qualificationStatus || null,
      prospectBucket: evaluation ? evaluation.bucket : opp.prospectBucket || null,
      evaluation,
      rationale: buildProspectRationale(opp),
      signals: (opp.signals || []).map((s) => normalizeBuyingSignal(s, opp.name)).filter(Boolean),
      unknowns: (opp.unknowns || [])
        .map((u) => (typeof u === 'object' ? u.text : String(u)))
        .filter(Boolean),
      intelligenceBrief: credibilityBrief,
      trust: credibilityBrief.trust,
      confidenceExplanation: credibilityBrief.confidenceExplanation,
      highestRemainingUnknowns: credibilityBrief.highestRemainingUnknowns,
      recommendedNextInvestigation: credibilityBrief.recommendedNextInvestigation,
    };
  }

  const rankedProspects = opportunities.map((opp, index) =>
    buildRankedProspectRow(opp, index, READINESS_STATES.READY)
  );
  for (let i = 0; i < fitCandidates.length; i += 1) {
    rankedProspects.push(
      buildRankedProspectRow(
        fitCandidates[i],
        rankedProspects.length,
        fitCandidates[i].readinessState || READINESS_STATES.UNKNOWN
      )
    );
  }
  for (let i = 0; i < watchCandidates.length; i += 1) {
    const row = watchCandidates[i];
    if (row.qualified !== true && row.qualificationStatus !== 'qualified') continue;
    rankedProspects.push(
      buildRankedProspectRow(
        row,
        rankedProspects.length,
        row.readinessState || READINESS_STATES.NOT_READY
      )
    );
  }

  const readinessOrder = {
    [READINESS_STATES.READY]: 0,
    [READINESS_STATES.UNKNOWN]: 1,
    [READINESS_STATES.NOT_READY]: 2,
  };
  rankedProspects.sort((a, b) => {
    const left = readinessOrder[a.readinessState] ?? 1;
    const right = readinessOrder[b.readinessState] ?? 1;
    if (left !== right) return left - right;
    return (Number(b.fit) || 0) - (Number(a.fit) || 0);
  });
  rankedProspects.forEach((row, index) => {
    row.rank = index + 1;
  });

  if (!rankedProspects.length && companies.length) {
    for (let i = 0; i < companies.length; i += 1) {
      const company = companies[i];
      rankedProspects.push({
        rank: i + 1,
        name: formatDiscoveryItem(company),
        id: company.id || null,
        readinessState: READINESS_STATES.UNKNOWN,
        rationale: 'Returned by Scout discovery.',
        signals: [],
        unknowns: [],
      });
    }
  }

  const confidenceBreakdown =
    artifact.confidenceBreakdown ||
    computeConfidenceBreakdown(
      opportunities,
      payload,
      artifact.confidence != null ? artifact.confidence : payload.confidence
    );

  const summary =
    (artifact.summary && String(artifact.summary).trim()) ||
    buildDiscoverySummary(opportunities, missionObjective);

  const blocked = artifact.blocked === true;

  const coverage = artifact.coverage || null;
  const discoveryStatus = artifact.discoveryStatus || null;
  const candidateUniverse = projectCandidateUniverseForContribution(
    Array.isArray(artifact.candidateUniverse)
      ? artifact.candidateUniverse
      : Array.isArray(payload.candidateUniverse)
        ? payload.candidateUniverse
        : []
  );
  const candidateUniverseCount =
    artifact.candidateUniverseCount != null
      ? Number(artifact.candidateUniverseCount)
      : candidateUniverse.length || null;
  const rankedProspectCount = rankedProspects.length;
  const readinessKnownCount =
    payload.readinessKnownCount != null
      ? Number(payload.readinessKnownCount)
      : Number(payload.readinessReadyCount || 0) + Number(payload.readinessNotReadyCount || 0);
  const excludedCount =
    payload.excludedCount != null
      ? Number(payload.excludedCount)
      : [
          ...watchCandidates,
          ...fitCandidates,
          ...candidateUniverse,
        ].filter((row) => row.excluded === true || row.prospectBucket === 'excluded').length;

  const estimatedMarket = artifact.estimatedMarket || null;
  const marketCoveragePct =
    artifact.marketCoveragePct != null ? artifact.marketCoveragePct : null;

  const publicMir = artifact.missionIntelligenceReport
    ? buildPublicMissionIntelligenceReport(artifact.missionIntelligenceReport)
    : null;

  const providerExecution = normalizeProviderExecution(
    artifact.providerExecution ||
      payload.providerExecution ||
      payload.providerReports ||
      []
  );

  const contribution = {
    companies,
    prospects,
    buyingSignals,
    decisionMakers,
    evidence,
    rankedProspects,
    confidence: confidenceBreakdown.overall,
    confidenceBreakdown,
    credibilityFramework: {
      version: 'SPEC-144',
      briefCount: rankedProspects.filter((r) => r.intelligenceBrief).length,
    },
    qualifiedCount,
    readinessReadyCount: payload.readinessReadyCount != null ? Number(payload.readinessReadyCount) : opportunities.length,
    readinessUnknownCount:
      payload.readinessUnknownCount != null
        ? Number(payload.readinessUnknownCount)
        : fitCandidates.length,
    readinessNotReadyCount: payload.readinessNotReadyCount != null ? Number(payload.readinessNotReadyCount) : 0,
    outcome: artifact.outcome || (blocked ? 'blocked' : 'completed'),
    blocked,
    summary,
    missionObjective,
    approvalConsumed: Boolean(artifact.approvalConsumed ?? opts.approvalConsumed),
    coverage,
    discoveryStatus,
    candidateUniverse,
    candidateUniverseCount,
    rankedProspectCount,
    readinessKnownCount,
    excludedCount,
    estimatedMarket,
    marketCoveragePct,
    discoveryReport: artifact.discoveryReport || null,
    discoveryConfidence: artifact.discoveryConfidence || null,
    discoveryPlan: artifact.discoveryPlan || null,
    missionIntelligenceReport: publicMir,
    discoveryArtifact: {
      spec: 'SPEC-173',
      fitCandidates: artifact.fitCandidates || [],
      watchCandidates: artifact.watchCandidates || [],
      businessUnderstanding: artifact.businessUnderstanding || null,
      businessJudgment: artifact.businessJudgment || null,
    },
    cognitiveTrace: artifact.cognitiveTrace || null,
    explainabilityGraph: artifact.explainabilityGraph || null,
    providerExecution,
  };

  if (containsForbiddenReasoningKeys(contribution)) {
    throw new Error('SPEC-173 boundary projection failed: forbidden reasoning keys remain in discovery contribution.');
  }

  return contribution;
}

/**
 * Whether discovery evidence is sufficient for operator prioritization approval.
 * @param {object} presentation
 * @returns {boolean}
 */
function hasSufficientEvidenceForPrioritization(presentation) {
  if (!presentation || presentation.blocked) return false;
  if (presentation.discoveryStatus === 'incomplete') return false;
  if (!presentation.rankedProspects || !presentation.rankedProspects.length) return false;
  if (!presentation.summary) return false;

  const evidenceItems = presentation.evidence || [];
  const hasProvenance = evidenceItems.some((e) => {
    if (typeof e === 'object') {
      const source = String(e.source || '');
      return source && !/test fixture/i.test(source);
    }
    const text = String(e || '').toLowerCase();
    return text && text !== 'fixture' && !/test fixture/i.test(text);
  });
  if (!hasProvenance) return false;

  const hasReadyProspects = presentation.rankedProspects.some(
    (row) => row.readinessState === READINESS_STATES.READY
  );
  if (!hasReadyProspects) {
    // ADR-101: qualified prospects with unknown readiness and provenance are prioritizable.
    return presentation.rankedProspects.some(
      (row) =>
        row.readinessState === READINESS_STATES.UNKNOWN ||
        row.readinessState == null
    );
  }

  const signals = presentation.buyingSignals || [];
  const hasSpecificSignals = signals.some((s) => {
    if (typeof s === 'object') return Boolean(s.label && s.type);
    return String(s).split(/\s+/).length >= 2;
  });

  return hasSpecificSignals;
}

module.exports = {
  normalizeScoutDiscoveryPayload,
  hasSufficientEvidenceForPrioritization,
  sourceLabel,
  formatSignalLabel,
  buildProspectRationale,
  buildOpportunityCredibilityBrief,
  computeConfidenceBreakdown,
};
