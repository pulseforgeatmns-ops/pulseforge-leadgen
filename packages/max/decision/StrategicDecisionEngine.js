'use strict';

/**
 * SPEC-165 — Strategic Decision Engine.
 * ADR-085 — Allocate finite effort toward the best business outcome.
 *
 * Opportunity Intelligence ranks what matters.
 * Strategic Decision allocates finite hours toward the mix that maximizes
 * the mission objective — with explicit tradeoffs.
 *
 * Pipeline: Opportunity Intelligence → Strategic Decision → Operator
 */

const {
  ACTIVITY_TYPES,
  ACTIVITY_LABELS,
  ACTIVITY_PRESENTATION_ORDER,
  ALLOCATION_KINDS,
  EXPECTED_ARR_USD,
  asText,
  roundHours,
  formatDuration,
  competingWorkLabel,
  buildResourceConstraints,
  buildAllocationBlock,
  buildExpectedBusinessOutcome,
  buildTradeoff,
} = require('./types');

function asList(value) {
  if (Array.isArray(value)) return value;
  if (value == null || value === false) return [];
  if (typeof value === 'number' && value > 0) return Array.from({ length: Math.min(20, Math.floor(value)) }, (_, i) => i + 1);
  if (typeof value === 'string' && value.trim()) return [value];
  if (value === true) return [true];
  return [];
}

function entityName(opportunity) {
  const row = opportunity || {};
  return asText(row.entity?.name || row.name || row.entity);
}

function levelOf(dimension) {
  if (!dimension) return 'medium';
  return asText(dimension.level || dimension).toLowerCase() || 'medium';
}

function isReachable(opportunity = {}) {
  const level = levelOf(opportunity.reachability);
  return level === 'high' || level === 'medium';
}

function isActiveOpportunity(opportunity = {}) {
  const category = asText(opportunity.category).toLowerCase();
  return category === 'immediate' || category === 'developing';
}

function notDeclined(opportunity = {}) {
  return asText(opportunity.category).toLowerCase() !== 'decline';
}

function estimateHoursRequired(opportunity = {}) {
  if (opportunity.estimatedHours != null) return roundHours(opportunity.estimatedHours);
  const category = asText(opportunity.category).toLowerCase();
  if (category === 'immediate') return 4;
  if (category === 'developing') return 2;
  return 1;
}

function estimateExpectedOutcome(opportunity = {}) {
  if (opportunity.estimatedARR != null || opportunity.expectedArr != null) {
    const arr = Number(opportunity.estimatedARR ?? opportunity.expectedArr);
    const confidence = Number(opportunity.confidence) || 0;
    return buildExpectedBusinessOutcome({ arr, confidence, expectedValue: Math.round(arr * confidence) });
  }
  const valueLevel = levelOf(opportunity.expectedBusinessValue);
  const arr = EXPECTED_ARR_USD[valueLevel] || EXPECTED_ARR_USD.medium;
  const confidence = Number(opportunity.confidence) || 0;
  return buildExpectedBusinessOutcome({
    arr,
    confidence,
    expectedValue: Math.round(arr * confidence),
  });
}

function missionObjectiveLabel(mission = {}) {
  const objectives = mission.objectives || mission.goals || [];
  if (Array.isArray(objectives) && objectives.length) return asText(objectives[0]);
  return asText(mission.objective || mission.name) || 'the mission objective';
}

function competingWorkItems(input = {}) {
  const raw = input.competingWork || input.mission?.competingWork || [];
  return asList(raw)
    .map((item) => {
      if (!item) return null;
      if (typeof item === 'string') {
        const type = item.trim().toLowerCase().replace(/\s+/g, '_');
        return { type, label: competingWorkLabel(type) };
      }
      const type = asText(item.type || item.activity || item.name).toLowerCase().replace(/\s+/g, '_');
      return { type, label: competingWorkLabel(item) || ACTIVITY_LABELS[type] || type };
    })
    .filter(Boolean);
}

function buildPros(opportunity = {}) {
  const pros = [];
  if (levelOf(opportunity.expectedBusinessValue) === 'high') {
    pros.push('Highest recurring value');
  } else if (levelOf(opportunity.expectedBusinessValue) === 'medium') {
    pros.push('Meaningful recurring value');
  }
  if (
    levelOf(opportunity.timing) === 'high' ||
    levelOf(opportunity.expectedProbability) === 'high' ||
    (opportunity.opportunityReasoning || []).some((r) => /buying|vendor instability|readiness|operations manager/i.test(r))
  ) {
    pros.push('Strong buying signals');
  }
  if (levelOf(opportunity.strategicFit) === 'high') {
    pros.push('Strong strategic fit with the mission');
  }
  if (levelOf(opportunity.expectedLearningValue) === 'high') {
    pros.push('High learning value for the beachhead');
  }
  if (opportunity.priority === 1) {
    if (!pros.includes('Highest recurring value') && !pros.some((p) => /priority/i.test(p))) {
      pros.push('Highest-ranked opportunity under current mission objectives');
    }
  }
  for (const reason of opportunity.opportunityReasoning || []) {
    if (pros.length >= 4) break;
    if (reason && !pros.includes(reason) && /recurring|buying|beachhead|growth|vendor/i.test(reason)) {
      if (!pros.some((p) => p.toLowerCase() === reason.toLowerCase())) {
        // keep dimension-level pros primary; skip raw heuristic dump
      }
    }
  }
  return pros.length ? pros : ['Advances the current mission objective'];
}

function buildCons({ opportunity, alternatives = [], competingWork = [], hoursRequired }) {
  const cons = [];
  const hours = roundHours(hoursRequired || estimateHoursRequired(opportunity));
  if (hours > 0) cons.push(`Consumes ${formatDuration(hours)}`);

  const next =
    alternatives.find(
      (o) => entityName(o) !== entityName(opportunity) && asText(o.category).toLowerCase() !== 'decline'
    ) ||
    alternatives.find((o) => entityName(o) !== entityName(opportunity)) ||
    alternatives[0];
  if (next && entityName(next)) {
    cons.push(`Delays ${entityName(next)}`);
  }

  for (const work of competingWork) {
    const label = (work.label || work.type || '').toLowerCase();
    if (label && !cons.some((c) => c.toLowerCase().includes(label))) {
      cons.push(`Delays ${work.label || competingWorkLabel(work.type)}`);
    }
  }

  return cons;
}

function evaluateTradeoff(input = {}) {
  const opportunity = input.opportunity;
  if (!opportunity) {
    return buildTradeoff({
      pros: [],
      cons: ['No opportunity selected for tradeoff analysis'],
      expectedOutcome: buildExpectedBusinessOutcome(),
    });
  }

  const alternatives = input.alternatives || [];
  const competingWork = competingWorkItems(input);
  const hoursRequired = estimateHoursRequired(opportunity);
  const expectedOutcome = estimateExpectedOutcome(opportunity);
  const delayed = [];
  const next = alternatives.find((o) => entityName(o) !== entityName(opportunity));
  if (next) delayed.push(entityName(next));
  for (const work of competingWork) delayed.push(work.label);

  return buildTradeoff({
    entity: entityName(opportunity),
    recommendedAction: opportunity.recommendedAction,
    hoursRequired,
    pros: buildPros(opportunity),
    cons: buildCons({ opportunity, alternatives, competingWork, hoursRequired }),
    expectedOutcome,
    confidence: expectedOutcome.confidence,
    delayed,
  });
}

function pendingProposalCount(input = {}) {
  const pending = input.pendingProposals;
  if (typeof pending === 'number') return pending;
  return asList(pending).length;
}

function hasScoutReviewNeed(input = {}, opportunities = []) {
  if (input.scoutDiscoveries === false) return false;
  if (input.scoutDiscoveries === true) return true;
  if (typeof input.scoutDiscoveries === 'number' && input.scoutDiscoveries > 0) return true;
  if (asList(input.scoutDiscoveries).length > 0) return true;
  if (asList(input.remainingUnknowns).length > 0) return true;
  return opportunities.length >= 5;
}

function contributionForPhone(opportunities = []) {
  const reachable = opportunities.filter((o) => isReachable(o) && notDeclined(o));
  return reachable.slice(0, 4).reduce((sum, o, index) => {
    const outcome = estimateExpectedOutcome(o);
    const capture = index === 0 ? 0.4 : 0.18;
    return sum + outcome.expectedValue * capture;
  }, 0);
}

function contributionForDoor(opportunities = []) {
  const local = opportunities.filter((o) => !isReachable(o) && notDeclined(o));
  return local.slice(0, 3).reduce((sum, o) => {
    const outcome = estimateExpectedOutcome(o);
    return sum + outcome.expectedValue * 0.22;
  }, 0);
}

function contributionForProposals(count) {
  if (count <= 0) return 0;
  return 900 * Math.min(count, 3);
}

function contributionForScoutReview(opportunities = [], remainingUnknowns = []) {
  const learningBoost = opportunities.filter((o) => levelOf(o.expectedLearningValue) === 'high').length * 200;
  return 400 + remainingUnknowns.length * 80 + Math.min(opportunities.length, 12) * 25 + learningBoost;
}

function delayPenalty({ alternatives = [], competingWork = [], pendingProposals = 0, opportunityCount = 0 }) {
  const next = alternatives[0];
  const nextCost = next ? estimateExpectedOutcome(next).expectedValue * 0.3 : 0;
  const mailCost = competingWork.some((w) => /direct_mail|direct mail/i.test(w.type || w.label || '')) ? 900 : 0;
  const otherWork = competingWork.filter((w) => !/direct_mail|direct mail/i.test(w.type || w.label || '')).length * 250;
  const proposalCost = pendingProposals > 0 ? 800 : 0;
  const crowdCost = Math.max(0, opportunityCount - 1) * 80;
  return nextCost + mailCost + otherWork + proposalCost + crowdCost;
}

function buildConcentratedAllocation(opportunity, constraints, mission) {
  if (!opportunity) return null;
  const hours = Math.min(estimateHoursRequired(opportunity), constraints.totalHours);
  if (hours <= 0) return null;
  const outcome = estimateExpectedOutcome(opportunity);
  const objective = missionObjectiveLabel(mission);
  return {
    kind: ALLOCATION_KINDS.CONCENTRATED,
    blocks: [
      buildAllocationBlock({
        activity: ACTIVITY_TYPES.OPPORTUNITY_PURSUIT,
        label: `Pursue ${entityName(opportunity)}`,
        hours,
        expectedContribution: outcome.expectedValue,
        reason: `Concentrating ${formatDuration(hours)} on ${entityName(opportunity)} because it is the highest expected contribution to ${objective}.`,
        opportunities: [entityName(opportunity)],
      }),
    ],
    expectedMissionOutcome: outcome.expectedValue,
    primaryEntity: entityName(opportunity),
  };
}

function buildMixedAllocation(input = {}, constraints, opportunities = []) {
  const pending = pendingProposalCount(input);
  const unknowns = asList(input.remainingUnknowns);
  const includeScout = hasScoutReviewNeed(input, opportunities);
  const reachable = opportunities.filter((o) => isReachable(o) && notDeclined(o));
  const unreachable = opportunities.filter((o) => !isReachable(o) && notDeclined(o));
  const active = opportunities.filter((o) => isActiveOpportunity(o) && notDeclined(o));
  const objective = missionObjectiveLabel(input.mission);

  const candidates = [];

  if (pending > 0) {
    candidates.push({
      activity: ACTIVITY_TYPES.PROPOSAL_FOLLOW_UP,
      hours: 0.5,
      expectedContribution: contributionForProposals(pending),
      reason: `Proposal follow-up converts work already in the pipeline — higher expected outcome per hour than opening a new opportunity.`,
      opportunities: asList(input.pendingProposals)
        .map((p) => asText(p.name || p.entity || p))
        .filter((n) => n && n !== 'true' && !/^\d+$/.test(n))
        .slice(0, 3),
    });
  }

  if (includeScout) {
    candidates.push({
      activity: ACTIVITY_TYPES.SCOUT_REVIEW,
      hours: 0.5,
      expectedContribution: contributionForScoutReview(opportunities, unknowns),
      reason: `Reviewing Scout discoveries reduces uncertainty across the remaining queue and protects ${objective} from acting on a stale picture.`,
      opportunities: [],
    });
  }

  let remainingAfterReserves = constraints.totalHours - candidates.reduce((sum, c) => sum + c.hours, 0);
  remainingAfterReserves = roundHours(Math.max(0, remainingAfterReserves));

  if (reachable.length && remainingAfterReserves > 0) {
    const defaultPhone = Math.min(2, remainingAfterReserves);
    const phoneHours = unreachable.length && remainingAfterReserves >= 1.5
      ? Math.min(2, roundHours(remainingAfterReserves * (2 / 3)))
      : defaultPhone;
    candidates.push({
      activity: ACTIVITY_TYPES.PHONE,
      hours: Math.max(0.5, phoneHours) || remainingAfterReserves,
      expectedContribution: contributionForPhone(opportunities),
      reason: `Phone time reaches ${entityName(reachable[0]) || 'the highest-value reachable opportunities'} plus other reachable opportunities faster than a full-day pursuit.`,
      opportunities: reachable.slice(0, 4).map(entityName),
    });
  }

  remainingAfterReserves = roundHours(
    constraints.totalHours - candidates.reduce((sum, c) => sum + (c.activity === ACTIVITY_TYPES.DOOR_KNOCKING ? 0 : c.hours), 0)
  );

  if (unreachable.length && remainingAfterReserves > 0) {
    const doorHours = Math.min(1, remainingAfterReserves);
    if (doorHours >= 0.5) {
      candidates.push({
        activity: ACTIVITY_TYPES.DOOR_KNOCKING,
        hours: doorHours,
        expectedContribution: contributionForDoor(opportunities),
        reason: `Door knocking covers high-value opportunities that phone cannot reach yet.`,
        opportunities: unreachable.slice(0, 3).map(entityName),
      });
    }
  }

  // If phone+door overshoot, trim door then phone so the day fits capacity.
  let used = candidates.reduce((sum, c) => sum + c.hours, 0);
  if (used > constraints.totalHours) {
    let overflow = roundHours(used - constraints.totalHours);
    for (let i = candidates.length - 1; i >= 0 && overflow > 0; i -= 1) {
      const cut = Math.min(candidates[i].hours, overflow);
      candidates[i].hours = roundHours(candidates[i].hours - cut);
      overflow = roundHours(overflow - cut);
    }
  }

  const blocks = candidates
    .filter((c) => c.hours > 0)
    .map((c) => buildAllocationBlock(c));

  // Pad leftover hours onto the highest-contribution remaining block (usually phone).
  const allocated = roundHours(blocks.reduce((sum, b) => sum + b.hours, 0));
  const leftover = roundHours(constraints.totalHours - allocated);
  if (leftover >= 0.5 && blocks.length) {
    const phoneIndex = blocks.findIndex((b) => b.activity === ACTIVITY_TYPES.PHONE);
    const idx = phoneIndex >= 0 ? phoneIndex : 0;
    const next = buildAllocationBlock({
      ...blocks[idx],
      hours: roundHours(blocks[idx].hours + leftover),
    });
    blocks[idx] = next;
  }

  const ordered = [...blocks].sort((a, b) => {
    if (b.hours !== a.hours) return b.hours - a.hours;
    return ACTIVITY_PRESENTATION_ORDER.indexOf(a.activity) - ACTIVITY_PRESENTATION_ORDER.indexOf(b.activity);
  });
  const expectedMissionOutcome = ordered.reduce((sum, b) => sum + b.expectedContribution, 0);

  return {
    kind: ALLOCATION_KINDS.MIXED,
    blocks: ordered,
    expectedMissionOutcome,
    primaryEntity: entityName(active[0] || opportunities[0]),
  };
}

function scoreNetOutcome(allocation, penalty) {
  if (!allocation) return -Infinity;
  if (allocation.kind === ALLOCATION_KINDS.CONCENTRATED) {
    return allocation.expectedMissionOutcome - penalty;
  }
  return allocation.expectedMissionOutcome;
}

function allocateResources(input = {}) {
  const opportunities = (input.opportunities || input.topOpportunities || []).filter(Boolean);
  const constraints = buildResourceConstraints(input.constraints || {});
  const competingWork = competingWorkItems(input);
  const pending = pendingProposalCount(input);
  const top = opportunities[0] || null;
  const alternatives = opportunities.slice(1);

  const concentrated = buildConcentratedAllocation(top, constraints, input.mission || {});
  const mixed = buildMixedAllocation(input, constraints, opportunities);
  const penalty = delayPenalty({
    alternatives,
    competingWork,
    pendingProposals: pending,
    opportunityCount: opportunities.length,
  });

  const mixedNet = scoreNetOutcome(mixed, 0);
  const concentratedNet = scoreNetOutcome(concentrated, penalty);
  const useMixed = !concentrated || mixedNet >= concentratedNet;
  const selected = useMixed ? mixed : concentrated;
  const alternative = useMixed ? concentrated : mixed;

  const comparison = {
    selectedKind: selected?.kind,
    mixedNet: Number(mixedNet.toFixed(2)),
    concentratedNet: Number(concentratedNet.toFixed(2)),
    delayPenalty: Number(penalty.toFixed(2)),
    summary: useMixed
      ? `A mixed day produces a higher expected mission outcome than concentrating ${constraints.totalHours} hours on ${entityName(top) || 'the top opportunity'} after opportunity cost.`
      : `Concentrating on ${entityName(top)} produces a higher expected mission outcome than spreading the day, even after delayed work.`,
  };

  return {
    constraints,
    opportunityCount: opportunities.length,
    selected,
    alternative,
    comparison,
    deferred: deferredWork({ selected, opportunities, competingWork, pending }),
  };
}

function deferredWork({ selected, opportunities = [], competingWork = [], pending = 0 }) {
  const deferred = [];
  const selectedNames = new Set((selected?.blocks || []).flatMap((b) => b.opportunities || []));
  const concentratedOn = selected?.kind === ALLOCATION_KINDS.CONCENTRATED ? selected.primaryEntity : null;

  if (concentratedOn) {
    for (const opp of opportunities.slice(1, 4)) {
      deferred.push(entityName(opp));
    }
    for (const work of competingWork) deferred.push(work.label);
    if (pending > 0) deferred.push(ACTIVITY_LABELS[ACTIVITY_TYPES.PROPOSAL_FOLLOW_UP]);
  } else {
    for (const work of competingWork) {
      const covered = (selected?.blocks || []).some((b) => b.activity === work.type);
      if (!covered) deferred.push(work.label);
    }
    for (const opp of opportunities.slice(0, 6)) {
      const name = entityName(opp);
      if (name && !selectedNames.has(name) && !isReachable(opp) && !(selected?.blocks || []).some((b) => b.activity === ACTIVITY_TYPES.DOOR_KNOCKING)) {
        deferred.push(name);
      }
    }
  }

  return [...new Set(deferred.filter(Boolean))];
}

function formatCapacityStatement(constraints, opportunityCount) {
  const aoLabel = `${constraints.availableAOs} AO${constraints.availableAOs === 1 ? '' : 's'}`;
  const hourLabel = `${constraints.availableHours} available hour${constraints.availableHours === 1 ? '' : 's'}`;
  const oppLabel = `${opportunityCount} opportunit${opportunityCount === 1 ? 'y' : 'ies'}`;
  return `You have ${aoLabel}, ${hourLabel}, ${oppLabel}. Here's the optimal allocation.`;
}

function explainAllocation(allocation, mission = {}) {
  if (!allocation?.blocks?.length) {
    return 'No allocation produced — insufficient opportunities or capacity.';
  }
  const objective = missionObjectiveLabel(mission);
  const lines = allocation.blocks.map((b) => `${b.duration} ${b.label}`);
  return [
    `Today's recommendation maximizes ${objective}.`,
    ...lines.map((line, i) => {
      const block = allocation.blocks[i];
      return `${line} — ${block.reason}`;
    }),
  ].join(' ');
}

function compareAllocations(selected, alternative) {
  if (!selected) {
    return { summary: 'No allocation to compare.' };
  }
  if (!alternative) {
    return {
      selectedKind: selected.kind,
      summary: `Only one feasible allocation: ${selected.kind}.`,
    };
  }
  return {
    selectedKind: selected.kind,
    selectedOutcome: selected.expectedMissionOutcome,
    alternativeKind: alternative.kind,
    alternativeOutcome: alternative.expectedMissionOutcome,
    summary: `Selected ${selected.kind} allocation (expected mission outcome ${Math.round(selected.expectedMissionOutcome)}) over ${alternative.kind} (${Math.round(alternative.expectedMissionOutcome)}).`,
  };
}

function overlayRecommendation(strategicDecision, fallback = {}) {
  if (!strategicDecision) return fallback;
  const tradeoff = strategicDecision.tradeoff;
  const allocation = strategicDecision.allocation;
  return {
    basedOnStrategicDecision: true,
    strategicAllocation: allocation,
    tradeoffs: tradeoff
      ? {
          entity: tradeoff.entity,
          pros: tradeoff.pros,
          cons: tradeoff.cons,
          expectedOutcome: tradeoff.expectedOutcome?.label,
          confidence: tradeoff.confidence,
          confidencePercent: tradeoff.confidencePercent,
        }
      : null,
    expectedBusinessOutcome: tradeoff?.expectedOutcome?.label || null,
    deferredWork: strategicDecision.deferred || [],
    capacityStatement: strategicDecision.capacityStatement,
    maximizesMissionObjective: true,
    notActivityBased: true,
    decisionSpec: 'SPEC-165',
    decisionAdr: 'ADR-085',
  };
}

function presentStrategicDecision(decision = {}) {
  const tradeoff = decision.tradeoff;
  return {
    spec: 'SPEC-165',
    adr: 'ADR-085',
    capacity: decision.capacityStatement,
    today: {
      heading: "Today's recommendation",
      blocks: (decision.allocation?.blocks || []).map((b) => ({
        duration: b.duration,
        hours: b.hours,
        activity: b.label,
        reason: b.reason,
      })),
    },
    ifPursued: tradeoff
      ? {
          heading: `If we pursue ${tradeoff.entity} today`,
          entity: tradeoff.entity,
          recommendedAction: tradeoff.recommendedAction,
          pros: tradeoff.pros,
          cons: tradeoff.cons,
          expectedOutcome: tradeoff.expectedOutcome?.label,
          confidence: `${tradeoff.confidencePercent}%`,
        }
      : null,
    deferred: decision.deferred || [],
    maximizesMissionObjective: true,
    notActivityBased: true,
  };
}

function buildStrategicDecision(input = {}) {
  const report = input.opportunityIntelligence || {};
  const opportunities =
    input.opportunities ||
    report.opportunities ||
    report.topOpportunities ||
    input.topOpportunities ||
    [];
  const constraints = buildResourceConstraints(input.constraints || {});
  const allocationResult = allocateResources({
    ...input,
    opportunities,
    constraints,
  });
  const top = opportunities[0] || null;
  const alternatives = opportunities.slice(1);
  const tradeoff = top
    ? evaluateTradeoff({
        opportunity: top,
        alternatives,
        competingWork: input.competingWork || input.mission?.competingWork,
        constraints,
      })
    : null;

  const selected = allocationResult.selected || { kind: ALLOCATION_KINDS.MIXED, blocks: [], expectedMissionOutcome: 0 };
  const capacityStatement = formatCapacityStatement(constraints, opportunities.length);
  const explanation = explainAllocation(selected, input.mission || {});
  const comparison = {
    ...allocationResult.comparison,
    ...compareAllocations(selected, allocationResult.alternative),
  };

  const decision = {
    kind: 'strategic_decision',
    spec: 'SPEC-165',
    adr: 'ADR-085',
    constraints,
    opportunityCount: opportunities.length,
    capacityStatement,
    allocation: selected,
    alternativeAllocation: allocationResult.alternative,
    comparison,
    tradeoff,
    deferred: allocationResult.deferred,
    expectedBusinessOutcome: tradeoff?.expectedOutcome || null,
    explanation,
    maximizesMissionObjective: true,
    notActivityBased: true,
    invariant:
      'Every daily recommendation is an allocation with tradeoffs, expected business outcome, and confidence. Activities are never recommended because they are inherently good.',
    summary: [
      capacityStatement,
      ...(selected.blocks || []).map((b) => `${b.duration} — ${b.label}`),
      tradeoff
        ? `If we pursue ${tradeoff.entity} today: ${tradeoff.pros.join('; ')}. Cons: ${tradeoff.cons.join('; ')}. Expected outcome ${tradeoff.expectedOutcome.label}. Confidence ${tradeoff.confidencePercent}%.`
        : null,
    ]
      .filter(Boolean)
      .join(' '),
  };

  decision.recommendationOverlay = overlayRecommendation(decision);
  return decision;
}

function attachStrategicDecision(missionReport = {}, input = {}) {
  const opportunities =
    input.opportunities ||
    missionReport.topOpportunities ||
    missionReport.opportunityIntelligence?.opportunities ||
    [];
  const decision = buildStrategicDecision({
    ...input,
    mission: input.mission || { id: missionReport.investigationState?.missionId, objectives: [] },
    opportunities,
    opportunityIntelligence: missionReport.opportunityIntelligence,
    remainingUnknowns: input.remainingUnknowns || missionReport.remainingUnknowns,
  });

  const recommendation = missionReport.recommendation
    ? { ...missionReport.recommendation, ...decision.recommendationOverlay }
    : { kind: 'strategic_decision', summary: decision.summary, ...decision.recommendationOverlay };

  return {
    ...missionReport,
    decisionSpec: 'SPEC-165',
    decisionAdr: 'ADR-085',
    strategicDecision: decision,
    recommendation,
    basedOnStrategicDecision: true,
  };
}

function ensureStrategicDecision(recommendation = {}, missionReport = {}, input = {}) {
  if (recommendation.basedOnStrategicDecision && recommendation.strategicAllocation?.blocks) {
    return recommendation;
  }
  const attached = attachStrategicDecision(missionReport, input);
  return {
    ...recommendation,
    ...attached.recommendation,
    summary: recommendation.summary || attached.recommendation.summary,
  };
}

module.exports = {
  estimateHoursRequired,
  estimateExpectedOutcome,
  evaluateTradeoff,
  allocateResources,
  buildStrategicDecision,
  attachStrategicDecision,
  ensureStrategicDecision,
  formatCapacityStatement,
  explainAllocation,
  compareAllocations,
  overlayRecommendation,
  presentStrategicDecision,
  delayPenalty,
  pendingProposalCount,
  hasScoutReviewNeed,
};
