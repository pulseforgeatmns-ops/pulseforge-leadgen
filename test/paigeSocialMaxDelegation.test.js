'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { createDelegation, executeDelegation, createMemoryStore } = require('../packages/max/specialistDelegation');
const { runPaigeSocialPublication } = require('../packages/max/specialistDelegation/PaigeSocialAdapter');

test('real Max delegation dispatches Paige with client, objective, channel, evidence and campaign', async () => {
  const store = createMemoryStore(); let input;
  const delegation = await createDelegation({ tenantId: '10', authorizedTenantId: '10', specialist: 'paige', capability: 'social_content',
    authority: 'draft', objective: 'Explain facilities assessments', reason: 'Anchor awareness', constraints: { allowedChannels: ['linkedin_page'] },
    targetContext: { entities: [{ kind: 'campaign', id: 'campaign-10' }] } }, { store });
  const result = await executeDelegation({ tenantId: '10', authorizedTenantId: '10', delegationId: delegation.id }, { store,
    adapterOpts: { generate: async i => { input = i; return { success: true, artifacts: [{ id: 'canonical-10', label: 'Anchor' }] }; } } });
  assert.equal(result.status, 'completed'); assert.equal(input.clientId, 10); assert.equal(input.channel, 'linkedin_page');
  assert.equal(input.contentObjective, 'Explain facilities assessments'); assert.equal(input.missionId, delegation.id); assert.equal(input.campaignId, 'campaign-10');
  assert.equal(result.artifactRefs[0].id, 'canonical-10'); assert.equal(input.publishNow, undefined);
});

test('Max approved publication uses exact artifact; execute and approval fabrication are not supported', async () => {
  let input;
  const d = { tenantId: '10', authority: 'execute_after_approval', targetContext: { entities: [{ kind: 'publication', id: 'a-10' }] } };
  await runPaigeSocialPublication(d, { publish: async i => { input = i; return { success: false, error: 'approval_binding_required' }; } });
  assert.equal(input.artifactId, 'a-10'); assert.equal(input.expectedApprovalHash, undefined);
  await assert.rejects(runPaigeSocialPublication({ ...d, authority: 'execute' }), /approved_execution_required/);
  await assert.rejects(createDelegation({ tenantId: '10', authorizedTenantId: '10', specialist: 'paige', capability: 'social_content_publish', authority: 'execute', objective: 'Publish', reason: 'Test' }, { store: createMemoryStore() }), /does not support/);
});
