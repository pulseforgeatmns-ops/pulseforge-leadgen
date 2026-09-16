'use strict';

/**
 * SPEC-256 — Canonical execution router for Paige social content generation.
 * Dashboard, cron, and CLI must invoke this path — never paigeAgent.run() directly.
 */

const pool = require('../db');
const { normalizeClientId } = require('../utils/clientContext');
const {
  BUILTIN_IDS,
  buildCapabilityContext,
  createCapabilityRunner,
  createCapabilityRegistry,
} = require('../packages/capabilities');
const { createSocialContentCapability } = require('../packages/capabilities/contentGeneration');

const CAPABILITY_ID = BUILTIN_IDS.SOCIAL_CONTENT;

let _registry = null;
let _runner = null;

function getRegistry() {
  if (!_registry) {
    _registry = createCapabilityRegistry();
    _registry.register(createSocialContentCapability({ pool }));
  }
  return _registry;
}

function getRunner() {
  if (!_runner) {
    _runner = createCapabilityRunner({ registry: getRegistry() });
  }
  return _runner;
}

function resetPaigeSocialContentExecutionForTests() {
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
 * Canonical router entry — manual, scheduled, and CLI generation share this path.
 * @param {object} input
 */
async function routePaigeSocialContentExecution(input = {}) {
  const { tenantId, clientId } = assertTenantInput(input);
  const dryRun = Boolean(input.dryRun ?? input.dry_run);
  const runner = getRunner();

  const runResult = await runner.run({
    capabilityId: CAPABILITY_ID,
    context: buildCapabilityContext({
      tenantId,
      clientId,
      missionId: input.missionId || input.mission_id || '',
      objective: input.contentObjective || input.objective || '',
      inputs: {
        tenantId,
        platform: input.platform || input.channel || null,
        channel: input.channel || input.platform || null,
        contentObjective: input.contentObjective || input.objective || null,
        workspaceContext: input.workspaceContext || input.workspace_context || null,
        missionContext: input.missionContext || input.mission_context || null,
        missionId: input.missionId || input.mission_id || null,
        evidence: input.evidence || [],
        cadenceContext: input.cadenceContext || input.cadence_context || null,
        dryRun,
        format: input.format || null,
        count: input.count || 1,
        simulateMiraUnavailable: input.simulateMiraUnavailable ?? input.simulate_mira_unavailable,
        invocationSource: input.invocationSource || input.source || input.channelSource || 'canonical_router',
      },
    }),
  });

  const capabilityResult = runResult.result || runResult;
  const generation = capabilityResult.outputs?.generation || null;
  const artifacts = capabilityResult.outputs?.artifacts || [];

  const capabilitySucceeded = capabilityResult.status === 'completed';
  return {
    spec: 'SPEC-256',
    success: capabilitySucceeded && generation?.success !== false,
    skipped: Boolean(generation?.skipped),
    reason: generation?.reason || null,
    dry_run: dryRun,
    client_id: clientId,
    tenant_id: tenantId,
    capability_id: CAPABILITY_ID,
    posts_generated: generation?.posts_generated ?? artifacts.length,
    channels_failed: generation?.channels_failed || [],
    outputs: generation?.outputs || [],
    artifacts,
    error: capabilityResult.errors?.[0]?.message || generation?.error || null,
    execution: {
      capabilityId: CAPABILITY_ID,
      status: capabilityResult.status,
      duration: capabilityResult.duration || 0,
    },
  };
}

module.exports = {
  CAPABILITY_ID,
  routePaigeSocialContentExecution,
  resetPaigeSocialContentExecutionForTests,
  getRegistry,
  getRunner,
};
