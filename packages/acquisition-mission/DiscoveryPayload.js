'use strict';

/**
 * SPEC-133 — Normalize Scout intelligence into AMO discovery contribution payloads.
 * Preserves evidence provenance, signal specificity, and confidence decomposition.
 */

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
 * @param {object} result - Scout intelligence result
 * @param {object} [opts]
 * @returns {object}
 */
function normalizeScoutDiscoveryPayload(result = {}, opts = {}) {
  const payload = result.payload || {};
  const opportunities = payload.opportunities || payload.acquisitionOpportunities || [];
  const missionObjective = opts.missionObjective || payload.missionObjective || null;

  const companies =
    payload.companies ||
    opportunities
      .map((row) => ({
        id: row.companyId || row.id,
        name: row.name,
        fit: row.fit,
        timing: row.timing,
        confidence: row.confidence,
      }))
      .filter((row) => row.id || row.name);

  const prospects = payload.prospects || payload.people || [];
  const qualifiedCount =
    payload.qualifiedCount != null
      ? Number(payload.qualifiedCount)
      : companies.length || opportunities.length;

  const buyingSignals = [];
  const seenSignals = new Set();
  for (const opp of opportunities) {
    for (const sig of opp.signals || []) {
      const normalized = normalizeBuyingSignal(sig, opp.name);
      if (!normalized) continue;
      const key = `${normalized.company}|${normalized.label}`;
      if (seenSignals.has(key)) continue;
      seenSignals.add(key);
      buyingSignals.push(normalized);
    }
  }
  for (const sig of payload.buyingSignals || payload.signals || []) {
    const normalized = normalizeBuyingSignal(sig);
    if (!normalized) continue;
    const key = normalized.label;
    if (seenSignals.has(key)) continue;
    seenSignals.add(key);
    buyingSignals.push(normalized);
  }

  const evidence = [];
  const seenEvidence = new Set();
  for (const opp of opportunities) {
    for (const ref of opp.evidenceRefs || []) {
      const normalized = normalizeEvidenceItem(ref, opp.name);
      if (!normalized) continue;
      const key = `${normalized.company}|${normalized.label}|${normalized.source}`;
      if (seenEvidence.has(key)) continue;
      seenEvidence.add(key);
      evidence.push(normalized);
    }
  }
  for (const ref of payload.evidence || payload.evidenceRefs || []) {
    const normalized = normalizeEvidenceItem(ref);
    if (!normalized) continue;
    const key = `${normalized.label}|${normalized.source}`;
    if (seenEvidence.has(key)) continue;
    seenEvidence.add(key);
    evidence.push(normalized);
  }

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

  const rankedProspects = opportunities.map((opp, index) => ({
    rank: index + 1,
    name: opp.name,
    id: opp.companyId || opp.id || null,
    fit: opp.fit != null ? Number(opp.fit) : null,
    timing: opp.timing != null ? Number(opp.timing) : null,
    confidence: opp.confidence != null ? Number(opp.confidence) : null,
    rationale: buildProspectRationale(opp),
    signals: (opp.signals || []).map((s) => normalizeBuyingSignal(s, opp.name)).filter(Boolean),
    unknowns: (opp.unknowns || [])
      .map((u) => (typeof u === 'object' ? u.text : String(u)))
      .filter(Boolean),
  }));

  if (!rankedProspects.length && companies.length) {
    for (let i = 0; i < companies.length; i += 1) {
      const company = companies[i];
      rankedProspects.push({
        rank: i + 1,
        name: formatDiscoveryItem(company),
        id: company.id || null,
        rationale: 'Returned by Scout discovery.',
        signals: [],
        unknowns: [],
      });
    }
  }

  const confidenceBreakdown = computeConfidenceBreakdown(
    opportunities,
    payload,
    payload.confidence != null ? payload.confidence : result.confidence
  );

  const summary =
    (result.summary && String(result.summary).trim()) ||
    (payload.summary && String(payload.summary).trim()) ||
    buildDiscoverySummary(opportunities, missionObjective);

  const blocked =
    result.status === 'blocked' ||
    qualifiedCount <= 0 ||
    payload.outcome === 'blocked';

  return {
    companies,
    prospects,
    buyingSignals,
    decisionMakers,
    evidence,
    rankedProspects,
    confidence: confidenceBreakdown.overall,
    confidenceBreakdown,
    qualifiedCount,
    outcome: result.status || (blocked ? 'blocked' : 'completed'),
    blocked,
    summary,
    missionObjective,
    approvalConsumed: Boolean(opts.approvalConsumed),
  };
}

/**
 * Whether discovery evidence is sufficient for operator prioritization approval.
 * @param {object} presentation
 * @returns {boolean}
 */
function hasSufficientEvidenceForPrioritization(presentation) {
  if (!presentation || presentation.blocked) return false;
  if (!presentation.rankedProspects || !presentation.rankedProspects.length) return false;
  if (!presentation.summary) return false;

  const signals = presentation.buyingSignals || [];
  const hasSpecificSignals = signals.some((s) => {
    if (typeof s === 'object') return Boolean(s.label && s.type);
    return String(s).split(/\s+/).length >= 2;
  });

  const evidenceItems = presentation.evidence || [];
  const hasProvenance = evidenceItems.some((e) => {
    if (typeof e === 'object') {
      return e.source && !/test fixture/i.test(String(e.source));
    }
    return e && String(e).toLowerCase() !== 'fixture';
  });

  return hasSpecificSignals && hasProvenance;
}

module.exports = {
  normalizeScoutDiscoveryPayload,
  hasSufficientEvidenceForPrioritization,
  sourceLabel,
  formatSignalLabel,
  buildProspectRationale,
  computeConfidenceBreakdown,
};
