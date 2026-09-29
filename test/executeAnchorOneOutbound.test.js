'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const {
  TENANT_ID,
  DEFAULT_MISSION_ID,
  MISSION_ID,
  parseArgs,
  resolveMissionId,
  run,
  prepareControlledSend,
  sendableQueueItems,
  selectHighestRankedSendable,
  compareCanonicalQueueOrder,
  buildPreSendPreview,
  assertCapacitySelectionMatch,
  assertMissionTenant,
  assertMissionReady,
  assertProbeMissionMatch,
  resolveCanonicalActiveCapacity,
} = require('../scripts/executeAnchorOneOutbound');
const { selectActiveCapacityContribution } = require('../scripts/lib/activeCapacitySelection');
const { MESSAGE_BINDING_SCOPES } = require('../packages/acquisition-mission/types');

const SCRIPT = path.join(__dirname, '..', 'scripts/executeAnchorOneOutbound.js');
const CRON = path.join(__dirname, '..', 'routes/cron.js');
const WORKFLOW = path.join(__dirname, '..', '.github/workflows/execute-anchor-one-outbound.yml');
const source = fs.readFileSync(SCRIPT, 'utf8');
const cronSource = fs.readFileSync(CRON, 'utf8');
const workflowSource = fs.readFileSync(WORKFLOW, 'utf8');

const LAW_FIRM_MISSION_ID = 'mission_ad7753b0-6def-441d-bb1a-3764656f5750';
const STR_MISSION_ID = 'mission_82e8102f-249c-4f44-b88e-2de76b13898e';
const OLD_CAPACITY_ID = 'contrib_55e11312-3837-4485-b95a-58134dcd7601';
const NEW_CAPACITY_ID = 'contrib_8258bb28-8150-4e8e-a7ff-9cd23bad1fdd';
const BLUE_DOOR_ID = 'ChIJ43Z_V2dP4okRCRcDHefV8OU';

function boundItem(overrides = {}) {
  const candidateId = overrides.candidateId || overrides.prospectId || overrides.id || BLUE_DOOR_ID;
  return {
    prospectId: candidateId,
    id: candidateId,
    candidateId,
    companyId: candidateId,
    email: 'sales@bluedoorliving.org',
    company: 'Blue Door Living Property Management',
    sendable: true,
    dnc: false,
    position: 1,
    ranking: { total: 0.92 },
    paige: {
      author: 'paige',
      source: 'paige',
      ready: true,
      subject: 'Turnover cleaning in Manchester',
      body: 'Hi — Anchor Cleaning helps STR operators keep units guest-ready.',
      variantLabel: 'Primary',
      candidateId,
      bindingScope: MESSAGE_BINDING_SCOPES.PROSPECT,
      attributableIntelligence: { companyName: 'Blue Door Living Property Management' },
    },
    ...overrides,
    paige: {
      author: 'paige',
      source: 'paige',
      ready: true,
      subject: 'Turnover cleaning in Manchester',
      body: 'Hi — Anchor Cleaning helps STR operators keep units guest-ready.',
      variantLabel: 'Primary',
      candidateId,
      bindingScope: MESSAGE_BINDING_SCOPES.PROSPECT,
      attributableIntelligence: { companyName: 'Blue Door Living Property Management' },
      ...(overrides.paige || {}),
    },
  };
}

function capacityPayload(items) {
  return {
    queue: { items },
    governor: { outcome: 'proceed' },
  };
}

function greenProbe(missionId = STR_MISSION_ID, capacityId = NEW_CAPACITY_ID) {
  return {
    missionId,
    capacityContributionId: capacityId,
    firstBlocker: null,
    autosendEnabled: false,
    enabledAgents: ['scout'],
    spec212: { valid: true, emailBearingSendableCount: 1 },
  };
}

function prepareDeps({
  missionId = STR_MISSION_ID,
  tenantId = '10',
  stage = 'ready',
  items = [boundItem()],
  probe = greenProbe(missionId),
  capacityId = NEW_CAPACITY_ID,
} = {}) {
  return {
    db: { query: async () => ({ rows: [] }) },
    loadMissionRow: async () => ({ id: missionId, tenant_id: tenantId, stage, payload: {} }),
    probeRun: async () => probe,
    loadActiveCapacity: async () => ({
      mission_id: missionId,
      capacity_id: capacityId,
      payload: capacityPayload(items),
    }),
  };
}

function fakeRuntime({ mission, contributions, capacityId = NEW_CAPACITY_ID }) {
  const inspect = () => ({
    mission,
    contributions: contributions || [
      {
        id: 'contrib_paige',
        specialist: 'paige',
        kind: 'variants',
        payload: {
          variants: [{
            candidateId: BLUE_DOOR_ID,
            label: 'Primary',
            subject: 'Turnover cleaning in Manchester',
            body: 'Hi — Anchor Cleaning helps STR operators keep units guest-ready.',
          }],
        },
      },
      {
        id: capacityId,
        specialist: 'emmett',
        kind: 'capacity',
        payload: capacityPayload([boundItem()]),
      },
      {
        id: 'contrib_approval',
        specialist: 'operator',
        kind: 'approval',
        payload: { emmettContributionId: capacityId },
      },
    ],
  });
  const engine = {
    get: (id, tenantId) => {
      if (String(id) !== String(mission.id)) return null;
      if (String(tenantId) !== String(mission.tenantId)) return null;
      return mission;
    },
    inspect,
    store: { addExecutionRecord() {} },
  };
  return {
    hydrate: async () => {},
    engine: () => engine,
    persistOpts: () => ({ persist: true }),
  };
}

describe('executeAnchorOneOutbound — one controlled production send', () => {
  it('is tenant 10 / expected mission and refuses without --confirm-production', async () => {
    assert.equal(TENANT_ID, '10');
    assert.equal(DEFAULT_MISSION_ID, LAW_FIRM_MISSION_ID);
    assert.equal(MISSION_ID, LAW_FIRM_MISSION_ID);
    assert.equal(parseArgs([]).confirmProduction, false);
    assert.equal(parseArgs(['--confirm-production']).confirmProduction, true);
    await assert.rejects(
      () => run({ confirmProduction: false }),
      (err) => err.code === 'confirm_production_required'
    );
  });

  it('accepts an explicit --mission-id and keeps the conservative default', () => {
    assert.equal(parseArgs([]).missionId, LAW_FIRM_MISSION_ID);
    assert.equal(
      parseArgs(['--confirm-production', '--mission-id', STR_MISSION_ID]).missionId,
      STR_MISSION_ID
    );
    assert.equal(resolveMissionId({ missionId: STR_MISSION_ID }), STR_MISSION_ID);
    assert.throws(
      () => parseArgs(['--confirm-production', '--mission-id']),
      (err) => err.code === 'mission_id_required'
    );
  });

  it('does not hardcode a stale CAPACITY contribution ID', () => {
    assert.doesNotMatch(source, /contrib_55e11312-3837-4485-b95a-58134dcd7601/);
    assert.doesNotMatch(source, /EXPECTED_CAPACITY_ID/);
    assert.match(source, /loadActiveCapacityForMission/);
    assert.match(source, /selectActiveCapacityContribution/);
    assert.match(source, /capacity_mismatch/);
  });

  it('does not hardcode stale law-firm targeting as the production path', () => {
    assert.match(source, /--mission-id/);
    assert.match(source, /DEFAULT_MISSION_ID/);
    assert.match(source, /resolveMissionId/);
    assert.match(source, /assertProbeMissionMatch\(probeBefore, missionId\)/);
    assert.match(source, /mission_82e8102f-249c-4f44-b88e-2de76b13898e/);
    assert.doesNotMatch(source, /const MISSION_ID = 'mission_ad7753b0-6def-441d-bb1a-3764656f5750'/);
    assert.match(cronSource, /handleExecuteAnchorOneOutboundCron[\s\S]{0,800}missionId/);
    assert.match(workflowSource, /--mission-id "\$\{\{ inputs\.mission_id \}\}"/);
  });

  it('uses the canonical approve then execute path and never mutates activation', () => {
    assert.match(source, /APPROVE_EXECUTION/);
    assert.match(source, /EXECUTE_OUTBOUND/);
    assert.match(source, /maxSends:\s*1/);
    assert.match(source, /prospectId:\s*selectedProspectId/);
    assert.match(source, /probe\.run|probeRun/);
    assert.match(source, /runtime\.persistOpts\(\{\s*persist:\s*true\s*\}\)/);
    assert.doesNotMatch(source, /UPDATE\s+clients/i);
    assert.doesNotMatch(source, /SET\s+autosend_enabled/i);
    assert.doesNotMatch(source, /array_append\(\s*enabled_agents/i);
    assert.match(source, /ALLOW_FIXTURE_FALLBACK === 'true'/);
    assert.match(source, /allowFixtureFallback:\s*false/);
    assert.match(source, /createProviderCounter/);
    assert.match(source, /preSend/);
  });

  it('classifies sendable queue items without inventing recipients', () => {
    const sendable = sendableQueueItems({
      queue: {
        items: [
          { prospectId: 'a', email: 'a@example.com', sendable: true, paige: { candidateId: 'a' } },
          { prospectId: 'b', email: 'b@example.com', sendable: false },
          { prospectId: 'c', dnc: true, email: 'c@example.com' },
          { prospectId: 'd' },
        ],
      },
    });
    assert.deepEqual(sendable.map((row) => row.prospectId), ['a']);
  });

  it('prefers the highest-ranked sendable item by canonical queue order', () => {
    const selected = selectHighestRankedSendable(capacityPayload([
      boundItem({
        prospectId: 'lower',
        id: 'lower',
        candidateId: 'lower',
        position: 2,
        ranking: { total: 0.4 },
        email: 'lower@example.com',
        paige: { candidateId: 'lower', subject: 'L', body: 'L', bindingScope: 'prospect', attributableIntelligence: {} },
      }),
      boundItem({
        prospectId: BLUE_DOOR_ID,
        id: BLUE_DOOR_ID,
        candidateId: BLUE_DOOR_ID,
        position: 1,
        ranking: { total: 0.9 },
        email: 'sales@bluedoorliving.org',
      }),
      boundItem({
        prospectId: 'blocked',
        id: 'blocked',
        candidateId: 'blocked',
        position: 0,
        ranking: { total: 1 },
        sendable: false,
        email: 'blocked@example.com',
        paige: { candidateId: 'blocked', subject: 'B', body: 'B', bindingScope: 'prospect', attributableIntelligence: {} },
      }),
    ]));
    assert.equal(selected.prospectId, BLUE_DOOR_ID);
    assert.ok(compareCanonicalQueueOrder(
      { position: 1, ranking: { total: 0.2 } },
      { position: 2, ranking: { total: 0.9 } }
    ) < 0);
  });

  it('prints recipient/company/subject/body from the selected item', () => {
    const preview = buildPreSendPreview(boundItem());
    assert.equal(preview.recipient, 'sales@bluedoorliving.org');
    assert.equal(preview.company, 'Blue Door Living Property Management');
    assert.equal(preview.subject, 'Turnover cleaning in Manchester');
    assert.match(preview.body, /Anchor Cleaning/);
    assert.equal(preview.candidateId, BLUE_DOOR_ID);
  });

  it('assertCapacitySelectionMatch aborts when probe and canonical durable CAPACITY differ', () => {
    assert.throws(
      () => assertCapacitySelectionMatch({
        expected: NEW_CAPACITY_ID,
        actual: OLD_CAPACITY_ID,
        context: 'Probe CAPACITY selection vs canonical durable state',
      }),
      (err) => err.code === 'capacity_mismatch'
        && err.message.includes(NEW_CAPACITY_ID)
        && err.message.includes(OLD_CAPACITY_ID)
    );
  });

  it('rejects a mission that does not belong to tenant 10', async () => {
    assert.throws(
      () => assertMissionTenant({ id: STR_MISSION_ID, tenant_id: '1' }, '10'),
      (err) => err.code === 'wrong_tenant'
    );
    await assert.rejects(
      () => prepareControlledSend(
        { confirmProduction: true, missionId: STR_MISSION_ID },
        prepareDeps({ tenantId: '1' })
      ),
      (err) => err.code === 'wrong_tenant'
    );
  });

  it('rejects a non-READY mission', async () => {
    assert.throws(
      () => assertMissionReady({ id: STR_MISSION_ID, stage: 'prepare' }),
      (err) => err.code === 'tme_wrong_stage'
    );
    await assert.rejects(
      () => prepareControlledSend(
        { confirmProduction: true, missionId: STR_MISSION_ID },
        prepareDeps({ stage: 'execute' })
      ),
      (err) => err.code === 'tme_wrong_stage'
    );
  });

  it('rejects when the readiness probe selected a different mission', async () => {
    assert.throws(
      () => assertProbeMissionMatch(greenProbe(LAW_FIRM_MISSION_ID), STR_MISSION_ID),
      (err) => err.code === 'mission_mismatch'
    );
    await assert.rejects(
      () => prepareControlledSend(
        { confirmProduction: true, missionId: STR_MISSION_ID },
        prepareDeps({ probe: greenProbe(LAW_FIRM_MISSION_ID) })
      ),
      (err) => err.code === 'mission_mismatch'
    );
  });

  it('rejects when there are zero sendable email-bearing queue items', async () => {
    await assert.rejects(
      () => prepareControlledSend(
        { confirmProduction: true, missionId: STR_MISSION_ID },
        prepareDeps({
          items: [
            boundItem({ sendable: false }),
            boundItem({
              prospectId: 'dnc',
              id: 'dnc',
              candidateId: 'dnc',
              dnc: true,
              paige: { candidateId: 'dnc', subject: 'X', body: 'X', bindingScope: 'prospect', attributableIntelligence: {} },
            }),
            boundItem({
              prospectId: 'no-email',
              id: 'no-email',
              candidateId: 'no-email',
              email: '',
              paige: { candidateId: 'no-email', subject: 'X', body: 'X', bindingScope: 'prospect', attributableIntelligence: {} },
            }),
          ],
        })
      ),
      (err) => err.code === 'empty_capacity_queue'
    );
  });

  it('accepts an explicit READY tenant-10 mission that matches the probe', async () => {
    const prepared = await prepareControlledSend(
      { confirmProduction: true, missionId: STR_MISSION_ID },
      prepareDeps()
    );
    assert.equal(prepared.missionId, STR_MISSION_ID);
    assert.equal(prepared.selectedProspectId, BLUE_DOOR_ID);
    assert.equal(prepared.canonicalCapacityId, NEW_CAPACITY_ID);
    assert.equal(prepared.spec212.valid, true);
    assert.equal(prepared.sendable.length, 1);
  });

  it('resolveCanonicalActiveCapacity selects newest revision-bound CAPACITY, not a fixed ID', async () => {
    const missionBody = {
      objective: 'Anchor law firms',
      revisionState: { emmettContributionId: NEW_CAPACITY_ID },
    };
    const pool = {
      query(sql) {
        if (sql.includes('FROM acquisition_missions')) {
          return { rows: [{ mission_id: MISSION_ID, payload: missionBody }] };
        }
        if (sql.includes('FROM acquisition_mission_contributions')) {
          return {
            rows: [
              {
                capacity_id: OLD_CAPACITY_ID,
                at: '2026-09-13T20:00:00.000Z',
                payload: { superseded: true, supersededBy: NEW_CAPACITY_ID },
              },
              {
                capacity_id: NEW_CAPACITY_ID,
                at: '2026-09-13T22:00:00.000Z',
                payload: { queue: { items: [] } },
              },
            ],
          };
        }
        throw new Error(`Unexpected query: ${sql}`);
      },
    };

    const active = await resolveCanonicalActiveCapacity(pool, TENANT_ID, MISSION_ID);
    assert.equal(active.capacity_id, NEW_CAPACITY_ID);
    assert.notEqual(active.capacity_id, OLD_CAPACITY_ID);

    const selected = selectActiveCapacityContribution(missionBody, [
      {
        capacity_id: OLD_CAPACITY_ID,
        at: '2026-09-13T20:00:00.000Z',
        payload: { superseded: true },
      },
      {
        capacity_id: NEW_CAPACITY_ID,
        at: '2026-09-13T22:00:00.000Z',
        payload: { queue: { items: [] } },
      },
    ]);
    assert.equal(selected.capacity_id, NEW_CAPACITY_ID);
  });

  it('executes only one item and suppresses replay without enabling autosend', async () => {
    const logs = [];
    const routeCalls = [];
    const providerCalls = [];
    const sentRecord = {
      id: 'amo_send_1',
      status: 'sent',
      prospectId: BLUE_DOOR_ID,
      providerMessageId: 'brevo-msg-1',
      executionIdentity: 'ident-1',
      idempotencyKey: 'exec_ident-1',
      payload: { email: 'sales@bluedoorliving.org' },
    };
    let durable = [];
    const mission = { id: STR_MISSION_ID, tenantId: '10', stage: 'ready' };
    const items = [
      boundItem({
        prospectId: 'second',
        id: 'second',
        candidateId: 'second',
        position: 2,
        ranking: { total: 0.1 },
        email: 'second@example.com',
        company: 'Second Co',
        paige: {
          candidateId: 'second',
          subject: 'Second',
          body: 'Second body',
          bindingScope: 'prospect',
          attributableIntelligence: { companyName: 'Second Co' },
        },
      }),
      boundItem({ position: 1, ranking: { total: 0.99 } }),
    ];

    const report = await run(
      { confirmProduction: true, missionId: STR_MISSION_ID },
      {
        assertRuntimeEnv() {},
        ...prepareDeps({ items }),
        getRuntime: () => fakeRuntime({ mission }),
        verifyEngineCapacityMatches: async () => ({ capacity_id: NEW_CAPACITY_ID }),
        listExecutions: async () => durable,
        findValidExecutionApproval: () => ({
          id: 'contrib_approval',
          payload: { emmettContributionId: NEW_CAPACITY_ID },
        }),
        sendEmail: async (input) => {
          providerCalls.push(input);
          return { messageId: 'brevo-msg-1' };
        },
        log: (line) => logs.push(line),
        routeIntent: async (input) => {
          routeCalls.push(input);
          if (input.intent === 'EXECUTE_OUTBOUND' && input.sendEmail) {
            if (providerCalls.length === 0) {
              await input.sendEmail({
                toEmail: 'sales@bluedoorliving.org',
                subject: 'Turnover cleaning in Manchester',
                idempotencyKey: 'exec_ident-1',
              });
              durable = [sentRecord];
              return {
                request: { id: 'req_execute' },
                routed: {
                  executionResult: {
                    transactionId: 'tx-1',
                    records: [sentRecord],
                  },
                },
              };
            }
            return {
              request: { id: 'req_replay' },
              routed: {
                executionResult: {
                  records: [{ ...sentRecord, deduplicated: true }],
                },
              },
            };
          }
          return {
            request: { id: 'req_approve' },
            routed: { executionResult: {} },
          };
        },
      }
    );

    assert.equal(report.missionId, STR_MISSION_ID);
    assert.equal(report.preSend.recipient, 'sales@bluedoorliving.org');
    assert.equal(report.preSend.company, 'Blue Door Living Property Management');
    assert.equal(report.subject, 'Turnover cleaning in Manchester');
    assert.match(report.body, /Anchor Cleaning/);
    assert.equal(report.approvalId, 'contrib_approval');
    assert.equal(report.persistedExecutionId, 'amo_send_1');
    assert.equal(report.brevoMessageId, 'brevo-msg-1');
    assert.equal(report.idempotency.providerCalls, 1);
    assert.equal(report.duplicateReplay.additionalProviderCalls, 0);
    assert.equal(report.duplicateReplay.suppressed, true);
    assert.equal(report.autosendEnabled, false);
    assert.deepEqual(report.enabledAgents, ['scout']);
    assert.match(report.verdict, /exactly once/);
    assert.equal(providerCalls.length, 1);
    assert.equal(routeCalls.filter((row) => row.intent === 'APPROVE_EXECUTION').length, 1);
    assert.equal(routeCalls.filter((row) => row.intent === 'EXECUTE_OUTBOUND').length, 2);
    assert.ok(routeCalls.every((row) => row.maxSends == null || row.maxSends === 1));
    assert.ok(routeCalls
      .filter((row) => row.intent === 'EXECUTE_OUTBOUND')
      .every((row) => row.prospectId === BLUE_DOOR_ID && row.maxSends === 1));
    assert.match(logs.join('\n'), /sales@bluedoorliving\.org/);
    assert.doesNotMatch(JSON.stringify(report), /autosendEnabled": true/);
  });
});
