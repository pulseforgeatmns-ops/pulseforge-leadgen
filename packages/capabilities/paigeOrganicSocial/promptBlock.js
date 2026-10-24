'use strict';

function buildOrganicPlanPromptBlock(plan) {
  if (!plan) return '';
  const assetLines = (plan.assets || []).length
    ? plan.assets.map((a) => `- ${a.filename} (${a.visualHints?.source || 'library'})`).join('\n')
    : '- none (text-only or no safe asset)';
  return `ANCHOR ORGANIC PLAN (SPEC-PAIGE-ANCHOR-SOCIAL-002):
Story concept: ${plan.storyConcept}
Content category: ${plan.contentCategory}
Proposed format: ${plan.proposedFormat}
Planning rationale: ${plan.planningRationale}
Scheduling rationale: ${plan.schedulingRationale || 'n/a'}
Asset truthfulness: ${plan.assetTruthfulness}
Selected media (reference only, not verified facts):
${assetLines}

Rules:
- Story first. Format second. Asset third.
- Never invent customer identity, property, location, service performed, results, reactions, or before/after relationships from imagery.
- If the format is text_only, do not imply a photo exists.
- Adapt copy to the destination platform; do not assume identical copy belongs everywhere.`;
}

module.exports = { buildOrganicPlanPromptBlock };
