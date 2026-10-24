'use strict';

/**
 * SPEC-100 — Max evaluates Scout results independently.
 * A completed Scout run is not automatically Max belief or elevated priority.
 */

const { evaluateSpecialistResult } = require('../specialistDelegation/Evaluator');
const { asText, clone, isTimely, COVERAGE_BANDS } = require('./Types');
const { deriveStateFromEvaluation } = require('./AcquisitionState');
const {
  investigationFromResult,
  coverageBand,
} = require('./InvestigationProvenance');

function opportunitiesFromResult(result) {
  if (!result) return [];
  if (result.payload && Array.isArray(result.payload.opportunities)) {
    return result.payload.opportunities;
  }
  return (result.artifactRefs || []).filter(
    (a) => a && a.kind === 'acquisition_opportunity'
  );
}

function classifyClaims(result, opportunities) {
  const accepted = [];
  const rejected = [];
  const unresolved = [];

  const push = (list, claim, source) => {
    if (!claim) return;
    list.push({
      kind: claim.kind || source,
      text: claim.text,
      entityId: claim.entityId || null,
      evidenceId: claim.evidenceId || null,
    });
  };

  for (const opp of opportunities) {
    for (const obs of opp.observations || []) push(accepted, obs, 'observation');
    for (const inf of opp.inferences || []) {
      rejected.push({
        kind: 'inference',
        text: inf.text,
        entityId: inf.entityId || null,
        reason: 'Inference is not accepted as an observed fact.',
      });
    }
    for (const unk of opp.unknowns || []) push(unresolved, unk, 'unknown');
  }

  for (const obs of result.observations || []) {
    if (obs.kind === 'inference') {
      rejected.push({
        kind: 'inference',
        text: obs.text,
        reason: 'Inference is not accepted as an observed fact.',
      });
    } else if (obs.kind === 'unknown') {
      push(unresolved, obs, 'unknown');
    }
  }

  for (const u of result.uncertainties || []) {
    unresolved.push({
      kind: 'unknown',
      text: typeof u === 'string' ? u : asText(u && u.text),
    });
  }

  return { acceptedClaims: accepted, rejectedClaims: rejected, unresolvedClaims: unresolved };
}

function knownCompanyIds(priorState) {
  if (!priorState) return new Set();
  const ids = new Set();
  for (const opp of priorState.opportunities || []) {
    if (opp.companyId) ids.add(String(opp.companyId));
  }
  return ids;
}

/**
 * Independent Max evaluation of a Scout SpecialistResult.
 *
 * @param {object} input
 * @returns {object}
 */
function evaluateScoutResult(input = {}) {
  const delegation = input.delegation;
  const result = input.result;
  const priorState = input.priorState || null;
  const base = evaluateSpecialistResult({
    delegation,
    result,
    operatorDirection: input.operatorDirection,
    suggestedPriorityChange: input.suggestedPriorityChange,
  });

  const opportunities = opportunitiesFromResult(result);
  const claims = classifyClaims(result, opportunities);
  const investigation = investigationFromResult(result);
  const coverageConfidence =
    investigation && investigation.coverageConfidence != null
      ? Number(investigation.coverageConfidence)
      : result.payload && result.payload.coverageConfidence != null
        ? Number(result.payload.coverageConfidence)
        : null;
  const coverage = coverageConfidence != null ? coverageBand(coverageConfidence) : COVERAGE_BANDS.WEAK;
  const timely = opportunities.filter((o) =>
    (o.signals || []).some(
      (s) =>
        ['portfolio_growth', 'expansion', 'new_location', 'hiring'].includes(s.type) &&
        isTimely(s.observedAt, input.now)
    )
  );
  const priorIds = knownCompanyIds(priorState);
  const newTimely = timely.filter((o) => !priorIds.has(String(o.companyId)));

  let materiality = 'immaterial';
  if (result.status === 'partial' && opportunities.length) {
    materiality = newTimely.length ? 'material' : 'partial';
  } else if (
    result.status === 'completed' &&
    base.operatorDirectionHonored &&
    newTimely.length > 0
  ) {
    materiality = 'material';
  } else if (result.status === 'completed' && opportunities.length === 0) {
    materiality = 'immaterial';
  } else if (result.status === 'completed' && timely.length === 0) {
    materiality = 'immaterial';
  } else if (result.status === 'completed' && timely.length && !newTimely.length) {
    materiality = 'immaterial';
  }

  const materialChange = materiality === 'material';
  const suggestedPriorityChange = materialChange
    ? base.suggestedPriorityChange || {
        domain: 'acquisition',
        from: (priorState && priorState.priorityImpact && priorState.priorityImpact.to) || 'normal',
        to: 'elevated',
        reason: result.summary,
      }
    : null;

  const evaluatedCount =
    investigation && investigation.coverage
      ? Number(investigation.coverage.candidatesEvaluated || 0)
      : opportunities.length;
  const marketAbsenceJustified =
    opportunities.length === 0 &&
    evaluatedCount > 0 &&
    coverage === COVERAGE_BANDS.STRONG;
  const comparativeClaimJustified =
    opportunities.length > 0 && coverage === COVERAGE_BANDS.STRONG;
  const conclusionTrust =
    coverage === COVERAGE_BANDS.STRONG
      ? 'high'
      : coverage === COVERAGE_BANDS.MODERATE
        ? 'moderate'
        : 'low';

  const explanation = buildMaxExplanation({
    delegation,
    result,
    materiality,
    timely,
    opportunities,
    claims,
    investigation,
    coverage,
    coverageConfidence,
    marketAbsenceJustified,
    comparativeClaimJustified,
  });

  const evaluation = {
    ...base,
    materialChange,
    materiality,
    suggestedPriorityChange,
    acceptedClaims: claims.acceptedClaims,
    rejectedClaims: claims.rejectedClaims,
    unresolvedClaims: claims.unresolvedClaims,
    acceptedAsGroundTruth: false,
    explanation,
    coverageConfidence,
    coverageBand: coverage,
    conclusionTrust,
    marketAbsenceJustified,
    comparativeClaimJustified,
    payload: {
      ...(base.payload || {}),
      materiality,
      acceptedClaims: claims.acceptedClaims,
      rejectedClaims: claims.rejectedClaims,
      unresolvedClaims: claims.unresolvedClaims,
      opportunityCount: opportunities.length,
      timelyCount: timely.length,
      newTimelyCount: newTimely.length,
      coverageConfidence,
      coverageBand: coverage,
      conclusionTrust,
      marketAbsenceJustified,
      comparativeClaimJustified,
      investigation,
    },
  };

  return evaluation;
}

function buildMaxExplanation(input) {
  const {
    delegation,
    result,
    materiality,
    timely,
    opportunities,
    claims,
    investigation,
    coverage,
    marketAbsenceJustified,
    comparativeClaimJustified,
  } = input;
  const unknownLine =
    (claims.unresolvedClaims[0] && claims.unresolvedClaims[0].text) ||
    (result.uncertainties && result.uncertainties[0]) ||
    null;
  const funnel = investigation && investigation.coverage ? investigation.coverage : null;
  const evaluated = funnel ? funnel.candidatesEvaluated : null;
  const basicFit = funnel ? funnel.basicFitCount : null;

  if (result.status === 'failed' || result.status === 'blocked') {
    if (evaluated === 0) {
      return (
        `Scout could not construct a candidate universe for this investigation. ` +
        `${result.summary || 'Candidate discovery provider unavailable.'} ` +
        `Zero companies evaluated is a discovery limitation, not evidence that no market opportunity exists. ` +
        `I am not elevating Acquisition.`
      );
    }
    const preserved = funnel
      ? ` Scout had already discovered ${funnel.candidatesDiscovered} and evaluated ${funnel.candidatesEvaluated} before the failure.`
      : ' Collected evidence was preserved.';
    return (
      `I could not treat this Scout run as a change in Acquisition. ` +
      `${result.summary || ''}${preserved}`
    );
  }

  if (opportunities.length === 0) {
    if (evaluated === 0) {
      return (
        `Scout could not construct a candidate universe for this investigation. ` +
        `Zero companies evaluated is a discovery limitation, not evidence that no market opportunity exists. ` +
        `I am not elevating Acquisition.`
      );
    }
    if (coverage === COVERAGE_BANDS.STRONG || marketAbsenceJustified) {
      return (
        `Scout investigated the current target segment thoroughly and didn't find any opportunities ` +
        `with enough current evidence for me to recommend pursuing.` +
        (evaluated != null
          ? ` He evaluated ${evaluated} business${evaluated === 1 ? '' : 'es'} across the current commercial acquisition criteria.`
          : '') +
        (basicFit
          ? ` ${basicFit} companies meet the target profile, but none had enough current timing evidence to qualify as a supported near-term opportunity. Current vendor timing is unknown.`
          : '') +
        ` I'm reasonably confident there isn't an obvious near-term pocket of demand under those criteria. ` +
        `I don't have enough evidence to elevate Acquisition.`
      );
    }
    return (
      `Scout didn't find a sufficiently supported opportunity, but I don't consider that strong evidence that none exist. ` +
      (evaluated != null
        ? `He evaluated ${evaluated} compan${evaluated === 1 ? 'y' : 'ies'}. `
        : '') +
      (basicFit
        ? `${basicFit} meet the target profile; current vendor timing is unknown. `
        : '') +
      `Coverage was ${coverage || 'limited'}, particularly around current timing signals, ` +
      `so I'd treat this as an incomplete investigation rather than a negative market conclusion. ` +
      `I am not elevating Acquisition.`
    );
  }

  if (materiality !== 'material') {
    return (
      `Scout found ${opportunities.length} matching compan${opportunities.length === 1 ? 'y' : 'ies'}, ` +
      `but nothing material changed in the near-term opportunity set. ` +
      `I am not elevating Acquisition.` +
      (unknownLine ? ` ${unknownLine}` : '')
    );
  }

  const completeness = comparativeClaimJustified
    ? ` Coverage was strong enough that these are a meaningful read of the current search space.`
    : ` These appear promising, but the investigation was not broad enough for me to claim they are the best or only opportunities in the market.`;

  return (
    `I elevated Acquisition because Scout identified ${opportunities.length} ` +
    `compan${opportunities.length === 1 ? 'y' : 'ies'} matching the current objective. ` +
    `${timely.length} show recent portfolio-growth or expansion signals. ` +
    `That materially improved the near-term opportunity set.` +
    completeness +
    (unknownLine
      ? ` ${unknownLine}`
      : ` We still don't have direct evidence that any are currently replacing a cleaning vendor.`)
  );
}

async function persistScoutEvaluation(input = {}, opts = {}) {
  const evaluation = evaluateScoutResult(input);
  if (opts.store && typeof opts.store.insertEvaluation === 'function') {
    const { newId } = require('../specialistDelegation/Store');
    evaluation.id = evaluation.id || newId();
    evaluation.createdAt = evaluation.createdAt || new Date().toISOString();
    const saved = await opts.store.insertEvaluation(evaluation);
    return { ...saved, ...evaluation, id: saved.id, createdAt: saved.createdAt };
  }
  return evaluation;
}

function shouldApplyPriority(evaluation) {
  return Boolean(
    evaluation &&
      evaluation.materialChange &&
      evaluation.suggestedPriorityChange &&
      evaluation.acceptedAsGroundTruth === false &&
      evaluation.operatorDirectionHonored !== false
  );
}

module.exports = {
  evaluateScoutResult,
  persistScoutEvaluation,
  opportunitiesFromResult,
  classifyClaims,
  shouldApplyPriority,
  deriveStateFromEvaluation,
  clone,
};
