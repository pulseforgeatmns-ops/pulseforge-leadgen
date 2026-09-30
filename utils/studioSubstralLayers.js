'use strict';

/**
 * Map Website Opportunity Intelligence findings into Studio Substral's six layers.
 * Preserves evidence_class and provenance — never upgrades INFERRED to MEASURED.
 */

const { LAYER_KEYS } = require('./studioSubstralAssessmentWorkflow');

const CATEGORY_TO_LAYER = Object.freeze({
  performance: 'performance',
  accessibility: 'accessibility',
  conversion_structure: 'conversion',
  conversion: 'conversion',
  technical_health: 'search',
  seo: 'search',
  search: 'search',
  business: 'trust',
  trust: 'trust',
  design: 'design',
  layout: 'design',
  diagnosis: 'design',
});

function emptyLayers() {
  return Object.fromEntries(LAYER_KEYS.map((key) => [key, []]));
}

function layerForFinding(finding) {
  const category = String(finding?.category || 'general').toLowerCase();
  if (CATEGORY_TO_LAYER[category]) return CATEGORY_TO_LAYER[category];
  if (/access/i.test(category)) return 'accessibility';
  if (/perf/i.test(category)) return 'performance';
  if (/convert|cta|contact/i.test(category)) return 'conversion';
  if (/trust|review|credential|license/i.test(category)) return 'trust';
  if (/design|layout|visual|typography/i.test(category)) return 'design';
  return 'search';
}

function buildSixLayerFindings(assessmentPayload = {}) {
  const layers = emptyLayers();
  const refs = []
    .concat(assessmentPayload.evidence_refs || [])
    .concat(assessmentPayload.assessment?.verified_findings || [])
    .concat(assessmentPayload.assessment?.inferred_findings || []);

  const seen = new Set();
  for (const finding of refs) {
    if (!finding || !finding.summary) continue;
    const key = `${finding.ref || finding.id}:${finding.summary}`;
    if (seen.has(key)) continue;
    seen.add(key);
    const layer = layerForFinding(finding);
    layers[layer].push({
      evidence_class: finding.evidence_class || 'UNKNOWN',
      summary: finding.summary,
      source: finding.source || null,
      ref: finding.ref || finding.id || null,
      category: finding.category || null,
    });
  }
  return layers;
}

function summarizeEvidenceStrength(sixLayerFindings) {
  let measured = 0;
  let observed = 0;
  let inferred = 0;
  let unknown = 0;
  for (const layer of LAYER_KEYS) {
    for (const row of sixLayerFindings[layer] || []) {
      switch (row.evidence_class) {
        case 'MEASURED': measured += 1; break;
        case 'OBSERVED': observed += 1; break;
        case 'INFERRED': inferred += 1; break;
        default: unknown += 1;
      }
    }
  }
  return { measured, observed, inferred, unknown, total: measured + observed + inferred + unknown };
}

module.exports = {
  buildSixLayerFindings,
  summarizeEvidenceStrength,
  layerForFinding,
  CATEGORY_TO_LAYER,
};
