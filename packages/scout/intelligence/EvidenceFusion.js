'use strict';

/**
 * SPEC-141 — Evidence Fusion.
 * One provider never owns truth. Scout combines evidence with provenance.
 */

function asText(value) {
  if (value == null) return '';
  return String(value).trim();
}

function sourceWeight(source) {
  const weights = {
    existing_pf: 0.85,
    google_maps: 0.9,
    google_places: 0.9,
    public_business_places: 0.88,
    linkedin: 0.82,
    hunter: 0.8,
    prospeo: 0.78,
    website: 0.75,
    company_website: 0.75,
    news: 0.7,
    county_records: 0.72,
    fixture: 0.5,
  };
  const key = asText(source).toLowerCase().replace(/[\s-]+/g, '_');
  return weights[key] != null ? weights[key] : 0.65;
}

/**
 * Normalize a raw evidence item into a fusion-ready record.
 * @param {object} raw
 * @param {object} candidate
 * @returns {object|null}
 */
function normalizeEvidenceItem(raw, candidate) {
  if (raw == null) return null;
  if (typeof raw === 'string') {
    const text = raw.trim();
    if (!text) return null;
    return {
      kind: 'observation',
      label: text,
      source: 'unknown',
      observedAt: null,
      weight: 0.6,
      provenance: { provider: 'unknown', raw: text },
    };
  }
  const snapshot = raw.snapshot && typeof raw.snapshot === 'object' ? raw.snapshot : {};
  const source =
    asText(snapshot.source || raw.source || raw.discoverySource || raw.sourceKind) ||
    'existing_repository';
  return {
    id: asText(raw.id || snapshot.evidenceId),
    kind: asText(raw.kind || snapshot.evidenceType || 'observation'),
    label:
      asText(raw.label) ||
      asText(snapshot.companyName) ||
      `${candidate.name || 'Company'} — ${source}`,
    source,
    observedAt: snapshot.observedAt || raw.observedAt || candidate.updatedAt || null,
    weight: sourceWeight(source),
    provenance: {
      provider: source,
      companyId: candidate.id,
      companyName: candidate.name,
      evidenceType: snapshot.evidenceType || raw.kind || null,
    },
  };
}

/**
 * Fuse evidence from multiple sources for one candidate.
 * @param {object} candidate
 * @param {object[]} rawEvidence
 * @returns {object}
 */
function fuseCandidateEvidence(candidate, rawEvidence = []) {
  const fromSignals = (candidate.signals || []).map((s) =>
    normalizeEvidenceItem(
      {
        label: s.label || s.text || s.type,
        source: s.source,
        observedAt: s.observedAt,
        kind: 'signal',
        snapshot: { source: s.source, observedAt: s.observedAt, evidenceType: s.type },
      },
      candidate
    )
  );

  const fromExplicit = (Array.isArray(rawEvidence) ? rawEvidence : []).map((e) =>
    normalizeEvidenceItem(e, candidate)
  );

  const fromWebsite =
    candidate.website && !fromExplicit.some((e) => e && e.source === 'website')
      ? [
          normalizeEvidenceItem(
            {
              label: `Website: ${candidate.website}`,
              source: 'website',
              kind: 'website',
            },
            candidate
          ),
        ]
      : [];

  const merged = [...fromSignals, ...fromExplicit, ...fromWebsite].filter(Boolean);
  const bySource = {};
  for (const item of merged) {
    if (!bySource[item.source]) bySource[item.source] = [];
    bySource[item.source].push(item);
  }

  const sources = Object.keys(bySource);
  const weights = merged.map((e) => e.weight);
  const confidence =
    weights.length > 0
      ? Number(
          Math.min(
            0.98,
            weights.reduce((sum, w) => sum + w, 0) / weights.length +
              Math.min(0.1, sources.length * 0.03)
          ).toFixed(2)
        )
      : 0.2;

  return {
    candidateId: candidate.id,
    companyName: candidate.name,
    evidence: merged,
    sources,
    confidence,
    provenance: merged.map((e) => ({
      source: e.source,
      label: e.label,
      observedAt: e.observedAt,
    })),
  };
}

module.exports = {
  sourceWeight,
  normalizeEvidenceItem,
  fuseCandidateEvidence,
};
