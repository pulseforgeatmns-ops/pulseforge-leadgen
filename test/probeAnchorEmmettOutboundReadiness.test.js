'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const {
  TENANT_ID,
  parseArgs,
  inspectSpec212,
  firstBlockerOf,
  buildReport,
  run,
} = require('../scripts/probeAnchorEmmettOutboundReadiness');
const { MESSAGE_BINDING_SCOPES } = require('../packages/acquisition-mission/types');

const SCRIPT = path.join(__dirname, '..', 'scripts', 'probeAnchorEmmettOutboundReadiness.js');
const source = fs.readFileSync(SCRIPT, 'utf8');
const dockerfile = fs.readFileSync(path.join(__dirname, '..', 'Dockerfile'), 'utf8');
const dockerignore = fs.readFileSync(path.join(__dirname, '..', '.dockerignore'), 'utf8');

function boundCapacityPayload() {
  return {
    capacity: { recommended: 1 },
    queue: {
      items: [{
        prospectId: 'co-harbor',
        email: 'alex@harborlaw.com',
        sendable: true,
        paige: {
          author: 'paige',
          source: 'paige',
          ready: true,
          variantLabel: 'Primary - Harbor Law',
          candidateId: 'co-harbor',
          bindingScope: MESSAGE_BINDING_SCOPES.PROSPECT,
          attributableIntelligence: { companyName: 'Harbor Law' },
        },
      }],
    },
    governor: { outcome: 'proceed' },
  };
}

describe('probeAnchorEmmettOutboundReadiness', () => {
  it('is tenant 10 and refuses without --confirm-production', async () => {
    assert.equal(TENANT_ID, '10');
    assert.equal(parseArgs([]).confirmProduction, false);
    assert.equal(parseArgs(['--confirm-production']).confirmProduction, true);
    await assert.rejects(
      () => run({ confirmProduction: false }),
      (err) => err.code === 'confirm_production_required'
    );
  });

  it('does not send, approve, execute, or mutate activation', () => {
    assert.doesNotMatch(source, /api\.brevo\.com\/v3\/smtp/);
    assert.doesNotMatch(source, /sendEmail\s*\(/);
    assert.doesNotMatch(source, /EXECUTION_INTENTS/);
    assert.doesNotMatch(source, /advanceExecuteOutbound/);
    assert.doesNotMatch(source, /advanceExecutionAfterApproval/);
    assert.doesNotMatch(source, /executeCanonical\s*\(/);
    assert.doesNotMatch(source, /routeExecutionRequest\s*\(/);
    assert.doesNotMatch(source, /UPDATE\s+clients/i);
    assert.doesNotMatch(source, /SET\s+autosend_enabled/i);
    assert.doesNotMatch(source, /array_append\(\s*enabled_agents/i);
    assert.match(source, /evaluateCanonicalSenderReadiness/);
    assert.match(source, /validateProspectMessageBindings/);
    assert.match(source, /getBrevoState/);
  });

  it('is copied into the production image at /app/scripts', () => {
    assert.match(dockerfile, /WORKDIR \/app/);
    assert.match(dockerfile, /COPY --chown=pptruser:pptruser \. \./);
    assert.doesNotMatch(dockerignore, /^scripts\/?$/m);
    assert.doesNotMatch(dockerignore, /^scripts\//m);
    assert.equal(fs.existsSync(SCRIPT), true);
  });

  it('validates SPEC-212 from a persisted CAPACITY shape without re-inject', () => {
    const wrapped = {
      id: 'contrib_capacity',
      specialist: 'emmett',
      kind: 'capacity',
      payload: boundCapacityPayload(),
    };
    const spec212 = inspectSpec212(wrapped);
    assert.equal(spec212.valid, true);
    assert.equal(spec212.blocker, null);
    assert.equal(spec212.queueCount, 1);

    const stripped = inspectSpec212({
      queue: {
        items: [{
          prospectId: 'co-harbor',
          paige: { author: 'paige', source: 'paige', ready: true },
        }],
      },
    });
    assert.equal(stripped.valid, false);
    assert.match(String(stripped.blocker), /binding/i);
  });

  it('reports the first blocker in the requested concise JSON shape', () => {
    const ready = buildReport({
      missionId: 'mission_ready',
      capacityContributionId: 'contrib_capacity',
      spec212: { valid: true, blocker: null, queueCount: 1 },
      senderReadiness: { sendable: true, blocker: null },
      brevo: {
        keyPresent: true,
        domainVerified: true,
        domainAuthenticated: true,
        senderActive: true,
      },
      autosendEnabled: false,
      enabledAgents: ['scout'],
    });
    assert.deepEqual(Object.keys(ready), [
      'missionId',
      'capacityContributionId',
      'spec212',
      'senderReadiness',
      'brevo',
      'autosendEnabled',
      'enabledAgents',
      'firstBlocker',
      'verdict',
    ]);
    assert.equal(ready.firstBlocker, null);
    assert.equal(ready.autosendEnabled, false);
    assert.deepEqual(ready.enabledAgents, ['scout']);
    assert.equal(ready.brevo.keyPresent, true);
    assert.equal(ready.spec212.valid, true);

    assert.equal(firstBlockerOf({
      missionId: 'mission_ready',
      capacityContributionId: 'contrib_capacity',
      spec212: { valid: false, blocker: 'missing_message_binding', queueCount: 1 },
      senderReadiness: { sendable: true },
    }), 'tme_message_binding_contamination');

    assert.equal(firstBlockerOf({
      missionId: 'mission_ready',
      capacityContributionId: 'contrib_capacity',
      spec212: { valid: true, queueCount: 1 },
      senderReadiness: { sendable: false, code: 'canonical_sender_not_ready' },
    }), 'canonical_sender_not_ready');
  });
});
