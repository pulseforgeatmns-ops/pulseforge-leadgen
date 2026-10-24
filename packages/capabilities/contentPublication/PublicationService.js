'use strict';
const { resolveSocialAccount } = require('./socialAccounts');
const { assertApproval, assertSocialCopy } = require('./approvalBinding');

function createPublicationService(deps = {}) {
  const registry = deps.adapterRegistry;
  if (!registry) throw new Error('adapter_registry_required');
  const resolveAccount = deps.accountResolver || resolveSocialAccount;
  async function prepare({ artifact, tenantId, clientId }) {
    if (!artifact || String(tenantId) !== artifact.tenantId || Number(clientId) !== artifact.clientId || String(clientId) !== String(tenantId)) throw new Error('tenant_scope_mismatch');
    const connected = await resolveAccount({ tenantId, clientId, platform: artifact.platform, accountId: artifact.approvalBinding?.account.id });
    assertApproval(artifact, connected.account);
    assertSocialCopy(artifact);
    const adapter = registry.resolve(artifact.platform);
    if (typeof adapter.readBack !== 'function') throw new Error('verified_publish_adapter_required');
    return { ...connected, adapter };
  }
  return {
    prepare,
    async publishApprovedArtifact(input) {
      const connected = await prepare(input);
      return connected.adapter.publish({ ...input, ...connected });
    },
    async readBack(input) {
      const connected = await prepare(input);
      return connected.adapter.readBack({ ...input, ...connected });
    },
  };
}
module.exports = { createPublicationService };
