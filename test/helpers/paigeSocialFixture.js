'use strict';
const { createInMemorySocialContentStore } = require('../../packages/capabilities/contentGeneration/SocialContentStore');
const { createSocialContentApprovalService } = require('../../packages/capabilities/contentGeneration/SocialContentApproval');
const { createSocialContentPublishCapability } = require('../../packages/capabilities/contentGeneration/SocialContentPublish');
const { PlatformAdapterRegistry } = require('../../packages/capabilities/contentPublication/PlatformAdapterRegistry');
async function fixture(opts = {}) {
  const store = opts.store || createInMemorySocialContentStore();
  const account = { id: 'anchor-linkedin', clientId: 10, platform: 'linkedin_page', provider: 'buffer', externalAccountId: 'channel-10', apiVersion: null };
  const scope = { tenantId: '10', clientId: 10 };
  const accountResolver = async input => {
    if (input.clientId !== account.clientId || (input.accountId && input.accountId !== account.id) || input.platform !== account.platform) throw new Error('social_account_not_connected');
    return { account: { ...account }, credentials: { accessToken: 'test-only' } };
  };
  let artifact = (await store.insertBatch([{ id: opts.id || require('crypto').randomUUID(), ...scope,
    platform: 'linkedin_page', label: 'Anchor social', body: 'A facilities assessment gives Manchester office managers a written scope before cleaning starts.',
    pendingCommentId: opts.pendingCommentId || require('crypto').randomUUID(), missionId: 'max-mission', meta: { campaignId: 'anchor-campaign' }, contentObjective: 'lead_generation' }]))[0];
  const approval = createSocialContentApprovalService({ socialContentStore: store, accountResolver });
  async function approve() {
    const p = await approval.preview({ ...scope, artifactId: artifact.id });
    const r = await approval.recordDecision({ ...scope, artifactId: artifact.id, decision: 'approve', accountId: account.id, expectedApprovalHash: p.approvalHash, approvedBy: 'operator:7' });
    artifact = r.artifact; return r;
  }
  let sends = 0, reads = 0;
  const adapter = { platform: account.platform,
    publish: async input => { sends++; return opts.send ? opts.send(input, sends) : { success: true, externalPostId: 'provider-10', externalAccountId: account.externalAccountId }; },
    readBack: async input => { reads++; return opts.read ? opts.read(input, reads) : { externalPostId: input.receipt.externalPostId, externalAccountId: account.externalAccountId,
      body: input.artifact.body, platformMatches: true, status: 'sent', published: true, publishedAt: '2026-09-24T03:00:00Z', externalUrl: 'https://linkedin.com/feed/update/test' }; },
  };
  const registry = new PlatformAdapterRegistry(); registry.register(adapter);
  const cap = createSocialContentPublishCapability({ socialContentStore: store, adapterRegistry: registry, accountResolver, liveEnabled: () => opts.enabled !== false, syncOutcome: opts.syncOutcome });
  const publish = inputs => cap.execute({ ...scope, inputs: { artifactId: artifact.id, ...inputs } });
  return { store, account, scope, artifact, approval, approve, publish, cap, adapter, accountResolver, counts: () => ({ sends, reads }), current: () => store.getById(artifact.id, '10', 10) };
}
module.exports = { fixture };
