'use strict';

const LAYERS = Object.freeze({
  PROPERTY_ACTIVITY: 'property_activity',
  POTENTIAL_FINANCING_NEED: 'potential_financing_need',
  VERIFIED_BORROWER_INTENT: 'verified_borrower_intent',
  QUALIFICATION: 'qualification',
});

const PERMIT_INTENT_LIMITATION =
  'A permit, renovation, reported cost, or ownership record is property activity. It does not establish verified borrower intent.';

function text(value) {
  return value == null ? '' : String(value).trim();
}

function money(value) {
  const raw = text(value).replace(/[$,]/g, '');
  if (!raw) return null;
  const amount = Number(raw);
  return Number.isFinite(amount) && amount > 0 ? amount : null;
}

function signal({
  layer,
  source,
  evidence,
  observedAt,
  confidence,
  limitations,
} = {}) {
  if (!Object.values(LAYERS).includes(layer)) return null;
  if (!text(source) || !text(evidence) || !text(observedAt)) return null;
  return Object.freeze({
    layer,
    source: text(source),
    evidence: text(evidence),
    observedAt: text(observedAt),
    confidence: text(confidence) || 'low',
    limitations: text(limitations) || PERMIT_INTENT_LIMITATION,
  });
}

function permitCost(row = {}) {
  return money(row.reported_cost) || money(row.estimated_cost) || money(row.initial_cost)
    || money(row.total_est_fee) || null;
}

function fromPermit({ row = {}, candidate = {}, permalink, observedAt, city } = {}) {
  const source = permalink || (candidate.acquisition_metadata && candidate.acquisition_metadata.sourceUrls
    && candidate.acquisition_metadata.sourceUrls[0]) || null;
  const stamp = observedAt || (candidate.acquisition_metadata && candidate.acquisition_metadata.evidenceDate);
  const address = candidate.service_area_match || candidate.company_location || city;
  const description = text(row.job_description || row.work_description);
  const jobType = text(row.job_type || row.permit_type);
  const signals = [];
  const activity = signal({
    layer: LAYERS.PROPERTY_ACTIVITY,
    source,
    evidence: [
      jobType && `${jobType} permit`,
      address && `at ${address}`,
      description && `(${description})`,
    ].filter(Boolean).join(' ') || 'Public renovation or alteration permit for an entity-owned property.',
    observedAt: stamp,
    confidence: 'high',
    limitations: PERMIT_INTENT_LIMITATION,
  });
  if (activity) signals.push(activity);

  const cost = permitCost(row);
  if (cost) {
    const need = signal({
      layer: LAYERS.POTENTIAL_FINANCING_NEED,
      source,
      evidence: `Public permit lists a reported or initial cost of $${Math.round(cost).toLocaleString('en-US')}.`,
      observedAt: stamp,
      confidence: 'low',
      limitations: 'A permit cost is not a financing request, not a loan amount, and not borrower intent.',
    });
    if (need) signals.push(need);
  } else if (/\b(rehab|renovation|alteration|interior)\b/i.test(`${jobType} ${description}`)) {
    const need = signal({
      layer: LAYERS.POTENTIAL_FINANCING_NEED,
      source,
      evidence: 'Public permit describes renovation, alteration, or rehab work that may later require capital.',
      observedAt: stamp,
      confidence: 'low',
      limitations: 'Work description is not a financing request. Capital need is inferred only, not verified.',
    });
    if (need) signals.push(need);
  }

  return Object.freeze({
    sourceType: 'property_record_signal',
    verifiedBorrowerIntent: false,
    signals: Object.freeze(signals.filter(Boolean)),
  });
}

function intentFromPublicRequest({ source, evidence, observedAt } = {}) {
  if (!text(source) || !text(evidence) || !text(observedAt)) {
    return Object.freeze({ verifiedBorrowerIntent: false, signals: Object.freeze([]) });
  }
  const verified = signal({
    layer: LAYERS.VERIFIED_BORROWER_INTENT,
    source,
    evidence,
    observedAt,
    confidence: 'medium',
    limitations: 'Intent is verified only when the public record itself requests financing. Ownership and permits are not intent.',
  });
  return Object.freeze({
    verifiedBorrowerIntent: Boolean(verified),
    signals: Object.freeze(verified ? [verified] : []),
  });
}

function qualificationSignal({ evaluation, source, observedAt } = {}) {
  const qualified = Boolean(evaluation && evaluation.qualified);
  return signal({
    layer: LAYERS.QUALIFICATION,
    source: source || 'approved_blueprint',
    evidence: qualified
      ? 'Record meets every approved qualification requirement currently on file.'
      : `Not qualified. Blockers: ${(evaluation && evaluation.blockers || []).join(', ') || 'unspecified'}.`,
    observedAt: observedAt || new Date().toISOString().slice(0, 10),
    confidence: qualified ? 'high' : 'high',
    limitations: 'Qualification uses only authorized Blueprint criteria. Missing criteria are not invented.',
  });
}

function summarizeLayers(signals = []) {
  const layers = {
    [LAYERS.PROPERTY_ACTIVITY]: false,
    [LAYERS.POTENTIAL_FINANCING_NEED]: false,
    [LAYERS.VERIFIED_BORROWER_INTENT]: false,
    [LAYERS.QUALIFICATION]: false,
  };
  for (const item of signals) {
    if (item && layers[item.layer] === false) layers[item.layer] = true;
  }
  return Object.freeze(layers);
}

module.exports = {
  LAYERS,
  PERMIT_INTENT_LIMITATION,
  signal,
  permitCost,
  fromPermit,
  intentFromPublicRequest,
  qualificationSignal,
  summarizeLayers,
};
