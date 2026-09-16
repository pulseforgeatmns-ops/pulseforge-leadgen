'use strict';

/**
 * SPEC-256 — Canonical execution router for Paige social content publication.
 */

const pool = require('../db');
const { normalizeClientId } = require('../utils/clientContext');
const {
  BUILTIN_IDS,
  buildCapabilityContext,
  createCapabilityRunner,
  createCapabilityRegistry,
} = require('../packages/capabilities');
const { createSocialContentPublishCapability } = require('../packages/capabilities/contentGeneration');

const CAPABILITY_ID = BUILTIN_IDS.SOCIAL_CONTENT_PUBLISH;

let _registry = null;
let _runner = null;

function getRegistry() {
  if (!_registry) {
    _registry = createCapabilityRegistry();
    _registry.register(createSocialContentPublishCapability({ pool }));
  }
  return _registry;
}

function getRunner() {
  if (!_runner) {
    _runner = createCapabilityRunner({ registry: getRegistry() });
  }
  return _runner;
}

function resetPaigeSocialContentPublicationForTests() {
  _registry = null;
  _runner = null;
}

function assertTenantInput(input = {}) {
  const clientId = normalizeClientId(input.client_id ?? input.clientId ?? input.tenantId);
  const tenantId = String(input.tenantId ?? input.tenant_id ?? clientId ?? '').trim();
  if (!tenantId || clientId == null) {
    throw new Error('tenant_scope_required');
  }
  if (tenantId !== String(clientId)) {
    throw new Error('tenant_client_mismatch');
  }
  return { tenantId, clientId };
}

/**
 * @param {object} input
 */
async function routePaigeSocialContentPublication(input = {}) {
  const { tenantId, clientId } = assertTenantInput(input);
  const artifactId = input.artifactId || input.artifact_id;
  if (!artifactId) throw new Error('artifact_id_required');

  const dryRun = Boolean(input.dryRun ?? input.dry_run);
  const runner = getRunner();
  const runResult = await runner.run({
    capabilityId: CAPABILITY_ID,
    context: buildCapabilityContext({
      tenantId,
      clientId,
      inputs: {
        tenantId,
        clientId,
        artifactId,
        dryRun,
        invocationSource: input.invocationSource || input.source || 'canonical_router',
      },
    }),
  });

  const capabilityResult = runResult.result || runResult;
  const published = capabilityResult.outputs?.published === true;
  const capabilitySucceeded = capabilityResult.status === 'completed';

  return {
    spec: 'SPEC-256',
    success: capabilitySucceeded && (published || capabilityResult.outputs?.idempotent === true || dryRun),
    dry_run: dryRun,
    client_id: clientId,
    tenant_id: tenantId,
    capability_id: CAPABILITY_ID,
    artifact_id: artifactId,
    artifact: capabilityResult.outputs?.artifact || null,
    channel: capabilityResult.outputs?.channel || null,
    idempotent: capabilityResult.outputs?.idempotent === true,
    error: capabilityResult.errors?.[0]?.message || null,
    execution: {
      capabilityId: CAPABILITY_ID,
      status: capabilityResult.status,
      duration: capabilityResult.duration || 0,
    },
  };
}

module.exports = {
  CAPABILITY_ID,
  routePaigeSocialContentPublication,
  resetPaigeSocialContentPublicationForTests,
  getRegistry,
  getRunner,
};
