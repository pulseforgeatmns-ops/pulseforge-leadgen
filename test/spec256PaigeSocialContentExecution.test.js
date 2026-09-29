'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { test, describe } = require('node:test');

const {
  createSocialContentCapability,
  createInMemorySocialContentStore,
  APPROVAL_STATES,
  PUBLISH_STATES,
  CAPABILITY_ID,
} = require('../packages/capabilities/contentGeneration');
const {
  routePaigeSocialContentExecution,
  resetPaigeSocialContentExecutionForTests,
} = require('../services/paigeSocialContentExecution');
const { BUILTIN_IDS } = require('../packages/capabilities/types');
const { createDefaultCapabilityRegistry } = require('../packages/max/specialistDelegation/CapabilityRegistry');

describe('SPEC-256 — Canonical Paige Social Content Execution', () => {
  test('registers social_content capability in builtin ids and Max delegation registry', () => {
    assert.equal(BUILTIN_IDS.SOCIAL_CONTENT, 'social_content');
    const registry = createDefaultCapabilityRegistry();
    const entry = registry.get('paige', 'social_content');
    assert.ok(entry);
    assert.equal(entry.callable, true);
    assert.equal(entry.adapter, 'paige_social_content');
  });

  test('production routes no longer invoke paigeAgent.run() directly', () => {
    const apiSource = fs.readFileSync(path.join(__dirname, '../routes/api.js'), 'utf8');
    const cronSource = fs.readFileSync(path.join(__dirname, '../routes/cron.js'), 'utf8');

    assert.match(apiSource, /routePaigeSocialContentExecution/);
    assert.match(cronSource, /routePaigeSocialContentExecution/);
    assert.doesNotMatch(apiSource, /agent === 'paige'\s*\?\s*await mod\.run\(/);
    const paigeCronBlock = cronSource.match(
      /else if \(agent === 'paige'\)\s*\{([\s\S]*?)\n\s*\} else if \(agent === 'paige_reflection'/
    );
    assert.ok(paigeCronBlock, 'expected dedicated cron paige branch');
    assert.doesNotMatch(paigeCronBlock[1], /mod\.run\(/);
  });

  test('manual and scheduled generation share the canonical router module', () => {
    const apiSource = fs.readFileSync(path.join(__dirname, '../routes/api.js'), 'utf8');
    const cronSource = fs.readFileSync(path.join(__dirname, '../routes/cron.js'), 'utf8');
    assert.match(apiSource, /services\/paigeSocialContentExecution/);
    assert.match(cronSource, /services\/paigeSocialContentExecution/);
  });

  test('outbound Paige AMO path remains unchanged', () => {
    const routerSource = fs.readFileSync(
      path.join(__dirname, '../packages/acquisition-mission/ExecutionRouter.js'),
      'utf8'
    );
    const executorSource = fs.readFileSync(
      path.join(__dirname, '../packages/max/workspace/PaigeVariantsExecutor.js'),
      'utf8'
    );
    assert.match(routerSource, /GENERATE_VARIANTS/);
    assert.match(executorSource, /runPaigeVariants/);
    assert.match(executorSource, /runPaigeVariants|CONTRIBUTION_KINDS\.VARIANTS/);
  });

  test('cross-tenant context leakage is rejected at router boundary', async () => {
    resetPaigeSocialContentExecutionForTests();
    await assert.rejects(
      () => routePaigeSocialContentExecution({ client_id: 1, tenantId: '2', dryRun: true }),
      /tenant_client_mismatch/
    );
  });

  test('capability rejects tenant/client mismatch', async () => {
    const store = createInMemorySocialContentStore();
    const cap = createSocialContentCapability({
      socialContentStore: store,
      runGeneration: async () => ({
        success: true,
        outputs: [{ company: 'Acme', channel: 'linkedin_page', content: 'Hello', content_type: 'educational', meta: {} }],
        drafts: [],
      }),
    });

    await assert.rejects(
      () => cap.execute({
        tenantId: '1',
        clientId: 2,
        inputs: { dryRun: true },
      }),
      /tenant_client_mismatch/
    );
  });

  test('failed generation does not commit canonical artifacts', async () => {
    const store = createInMemorySocialContentStore();
    const cap = createSocialContentCapability({
      socialContentStore: store,
      runGeneration: async () => ({
        success: false,
        error: 'model_unavailable',
        outputs: [],
        drafts: [],
      }),
    });

    const result = await cap.execute({
      tenantId: '5',
      clientId: 5,
      inputs: { dryRun: false },
    });
    assert.equal(result.status, 'failed');
    assert.equal((await store.listByTenant('5', 5)).length, 0);
  });

  test('generated artifacts remain PENDING_APPROVAL and never publish', async () => {
    const store = createInMemorySocialContentStore();
    const cap = createSocialContentCapability({
      socialContentStore: store,
      runGeneration: async () => ({
        success: true,
        outputs: [],
        drafts: [{
          company: { name: 'Pulseforge', industry: 'Agency', id: 1 },
          content: 'Draft body',
          contentType: 'educational',
          channel: 'linkedin_page',
          meta: { format: 'dialogue' },
        }],
      }),
      mirrorPendingComment: async () => 'pending-1',
    });

    const result = await cap.execute({
      tenantId: '1',
      clientId: 1,
      objective: 'awareness',
      inputs: {
        dryRun: false,
        contentObjective: 'awareness',
        invocationSource: 'test',
      },
    });

    assert.equal(result.status, 'completed');
    const artifacts = await store.listByTenant('1', 1);
    assert.equal(artifacts.length, 1);
    assert.equal(artifacts[0].approvalState, APPROVAL_STATES.PENDING_APPROVAL);
    assert.equal(artifacts[0].publishState, PUBLISH_STATES.NOT_PUBLISHED);
    assert.equal(artifacts[0].provenance.capabilityId, CAPABILITY_ID);
    assert.equal(artifacts[0].provenance.tenantId, '1');
    assert.equal(artifacts[0].provenance.platform, 'linkedin_page');
    assert.equal(artifacts[0].pendingCommentId, 'pending-1');
    assert.equal(result.outputs.artifacts[0].approvalState, APPROVAL_STATES.PENDING_APPROVAL);
  });

  test('generation payload cannot assert publish authority', () => {
    const { buildSocialContentArtifact } = require('../packages/capabilities/contentGeneration');
    assert.throws(
      () => buildSocialContentArtifact({
        tenantId: '1',
        clientId: 1,
        platform: 'linkedin_page',
        label: 'Test',
        body: 'Body',
        publish: true,
      }),
      /generation_forbids_publish_authority/
    );
  });

  test('dry-run returns drafts without store writes', async () => {
    resetPaigeSocialContentExecutionForTests();
    const store = createInMemorySocialContentStore();
    const cap = createSocialContentCapability({
      socialContentStore: store,
      runGeneration: async () => ({
        success: true,
        outputs: [{
          company: 'Acme',
          channel: 'linkedin_page',
          content: 'Draft only',
          content_type: 'educational',
          meta: {},
        }],
        drafts: [],
      }),
    });

    const { createCapabilityRunner, createCapabilityRegistry } = require('../packages/capabilities');
    const registry = createCapabilityRegistry();
    registry.register(cap);
    const runner = createCapabilityRunner({ registry });
    const runResult = await runner.run({
      capabilityId: BUILTIN_IDS.SOCIAL_CONTENT,
      context: {
        tenantId: '3',
        clientId: 3,
        inputs: { dryRun: true },
      },
    });

    assert.equal(runResult.result.status, 'completed');
    assert.equal((await store.listByTenant('3', 3)).length, 0);
    assert.equal(runResult.result.outputs.artifacts.length, 1);
  });
});
