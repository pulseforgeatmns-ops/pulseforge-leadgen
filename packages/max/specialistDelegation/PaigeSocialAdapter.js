'use strict';
async function runPaigeSocialContent(delegation, opts = {}) {
  if (delegation.authority !== 'draft') throw new Error('paige_draft_authority_required');
  const channels = delegation.constraints?.allowedChannels || [];
  if (channels.length !== 1) throw new Error('paige_specific_channel_required');
  const generate = opts.generate || require('../../../services/paigeSocialContentExecution').routePaigeSocialContentExecution;
  const result = await generate({ clientId: Number(delegation.tenantId), tenantId: delegation.tenantId,
    channel: channels[0], contentObjective: delegation.objective,
    missionId: delegation.id, campaignId: delegation.targetContext?.entities?.find(e => e.kind === 'campaign')?.id || null,
    evidence: delegation.evidenceRefs || [], missionContext: { delegationId: delegation.id, objective: delegation.objective },
    invocationSource: 'max_specialist_delegation' });
  return { status: result.success ? 'completed' : 'blocked', summary: result.success ? 'Paige drafted client content for human review.' : result.error || result.reason || 'Paige generation blocked.',
    actionsTaken: [{ text: 'Generated canonical drafts; publication requires exact artifact approval.' }],
    artifactRefs: (result.artifacts || []).map(a => ({ id: a.id, kind: 'social_content_draft', label: a.label })),
    evidenceRefs: delegation.evidenceRefs || [], observations: [], uncertainties: [],
    recommendedNextAction: { type: 'ask_operator', text: 'Review the canonical draft and destination account in Paige social review.' },
    errors: result.success ? [] : [{ code: 'generation_blocked', message: result.error || result.reason || 'generation_blocked' }],
    startedAt: new Date().toISOString(), completedAt: new Date().toISOString() };
}
module.exports = { runPaigeSocialContent };

async function runPaigeSocialPublication(delegation, opts = {}) {
  if (delegation.authority !== 'execute_after_approval') throw new Error('paige_approved_execution_required');
  const targets = (delegation.targetContext?.entities || []).filter(e => e.kind === 'publication');
  if (targets.length !== 1) throw new Error('specific_social_artifact_required');
  const publish = opts.publish || require('../../../services/paigeSocialContentPublication').routePaigeSocialContentPublication;
  const result = await publish({ tenantId: delegation.tenantId, clientId: Number(delegation.tenantId), artifactId: targets[0].id, invocationSource: 'max_specialist_delegation' });
  return { status: result.success ? 'completed' : 'blocked', summary: result.success ? 'Paige publication verified.' : result.error || 'Publication requires review.',
    actionsTaken: [{ text: 'Checked bound approval and ran governed publication or read-back.' }], artifactRefs: [{ id: targets[0].id, kind: 'social_content_draft' }],
    evidenceRefs: [], observations: [{ text: JSON.stringify(result.publication || {}) }], uncertainties: [],
    errors: result.success ? [] : [{ code: 'publication_blocked', message: result.error || 'publication_blocked' }],
    startedAt: new Date().toISOString(), completedAt: new Date().toISOString() };
}
module.exports.runPaigeSocialPublication = runPaigeSocialPublication;
