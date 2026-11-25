'use strict';

const { PROPOSED_MIN_AMOUNT, PROPOSED_MAX_AMOUNT } = require('./leviScoutProductionContract');
const { SLOT_IDS, inspectApprovedBlueprint } = require('./leviScoutBlueprintReadiness');
const { proposedRangeIsNotAuthority } = require('./leviScoutApprovedCriteria');

const RESEARCH_PROFILE = Object.freeze({
  financingAmountRange: Object.freeze({
    label: '$5,000–$20,000 research target',
    min: PROPOSED_MIN_AMOUNT,
    max: PROPOSED_MAX_AMOUNT,
  }),
  geographicCoverage: Object.freeze({
    label: 'nationwide rehabilitation coverage',
  }),
  eligiblePropertyTypes: Object.freeze({
    label: 'rehabilitation / equity-gap project types',
  }),
  borrowerEligibility: Object.freeze({
    label: 'real-estate operators seeking rehab capital',
  }),
  financingProductsAndExclusions: Object.freeze({
    label: 'rehab and equity-gap financing products',
  }),
});

function evidenceMentionsRange(evidence = [], min, max) {
  const needle = new RegExp(
    `financing\\s+range:\\s*\\$${Number(min).toLocaleString('en-US')}\\s*(?:-|–|—|to)\\s*\\$${Number(max).toLocaleString('en-US')}`,
    'i'
  );
  return evidence.some(item => needle.test(String(item.value || '')));
}

function evidenceMentions(evidence = [], pattern) {
  return evidence.some(item => pattern.test(String(item.value || '')));
}

function fieldStatus({ present, matchesResearch }) {
  if (!present) return 'research_only_not_approved';
  if (matchesResearch) return 'approved_and_matches_research';
  return 'approved_and_differs_from_research';
}

function reconcileResearchProfile(understanding, tenantId) {
  const report = inspectApprovedBlueprint(understanding, tenantId);
  const proposed = proposedRangeIsNotAuthority();
  const fields = SLOT_IDS.map((id) => {
    const slot = report.slots[id];
    const research = RESEARCH_PROFILE[id];
    let matchesResearch = false;
    if (id === 'financingAmountRange') {
      matchesResearch = Boolean(
        report.criteriaRange
        && report.criteriaRange.min === research.min
        && report.criteriaRange.max === research.max
      ) || evidenceMentionsRange(slot.evidence, research.min, research.max);
    } else if (id === 'geographicCoverage') {
      matchesResearch = evidenceMentions(slot.evidence, /nationwide/i);
    } else if (id === 'eligiblePropertyTypes') {
      matchesResearch = evidenceMentions(slot.evidence, /rehab|renovation|equity[ -]?gap/i);
    } else if (id === 'borrowerEligibility') {
      matchesResearch = evidenceMentions(slot.evidence, /operator|sponsor|investor|entity/i);
    } else if (id === 'financingProductsAndExclusions') {
      matchesResearch = evidenceMentions(slot.evidence, /rehab|equity[ -]?gap|bridge/i);
    }
    return Object.freeze({
      id,
      researchAssumption: research.label,
      approvedPresent: slot.present,
      approvedEvidenceCount: slot.evidence.length,
      approvedEvidenceRefs: slot.evidence.map(item => ({
        id: item.id,
        provenance: item.provenance,
      })),
      matchesResearchAssumption: slot.present ? matchesResearch : false,
      status: fieldStatus({ present: slot.present, matchesResearch }),
      promoted: false,
    });
  });
  return Object.freeze({
    trusted: report.trusted,
    knowledgeState: report.knowledgeState,
    blueprintSource: report.blueprintSource,
    invented: false,
    authorizedChanges: false,
    proposedRangeAuthorized: proposed.authorized,
    proposedRange: Object.freeze({
      minAmount: proposed.minAmount,
      maxAmount: proposed.maxAmount,
      authorized: false,
    }),
    criteriaRange: report.criteriaRange,
    fields,
    missingApprovedSlots: report.missing.map(item => item.id),
    questionsForLevi: report.questionsForLevi,
  });
}

module.exports = {
  RESEARCH_PROFILE,
  reconcileResearchProfile,
};
