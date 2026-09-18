'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const amo = require('../packages/acquisition-mission');
const {
  STAGES,
  SPECIALISTS,
  CONTRIBUTION_KINDS,
  OPERATOR_DECISION_KINDS,
  EXECUTION_INTENTS,
  EXECUTION_SOURCES,
  createExecutionRequest,
  routeExecutionRequest,
  applyStageTransition,
} = amo;
const {
  advancePlanAfterApproval,
  advanceDiscoveryAfterApproval,
  advancePrioritizationAfterApproval,
  advanceMaxPrioritization,
  advanceAcquisitionApproach,
  advancePaigeVariants,
  advanceEmmettCapacity,
} = require('../packages/max/workspace/AmoOperatorApproval');
const {
  classifyMissionEligibility,
  detectCustomerSend,
  validatePaigeVariantsDoctrine,
  paigePayloadFailsDoctrine,
} = require('../scripts/lib/anchorPaigeCopyRevision');
const {
  parseArgs,
  run,
  assertRuntimeEnv,
} = require('../scripts/regenerateAnchorPaigeCopyRevision');
const { buildDefaultColdEmail, validateAnchorCopyDoctrine } = require('../utils/anchorCopyDoctrine');

const OBJECTIVE =
  'Acquire one recurring commercial cleaning client from a short-term rental operator in Greater Manchester area.';

async function preparedReadyMission() {
  const engine = amo.createAcquisitionMissionEngine();
  const mission = engine.create({
    tenantId: '10',
    objective: OBJECTIVE,
    targetSegment: 'Short-term rental operators',
  });
  await advancePlanAfterApproval({ engine, mission, tenantId: '10', question: 'Approved.' });
  await advanceDiscoveryAfterApproval({
    engine, mission, tenantId: '10', question: 'Approved.', allowFixtureFallback: true,
  });
  await advancePrioritizationAfterApproval({
    engine, mission: engine.get(mission.id, '10'), tenantId: '10', question: 'Approved.',
  });
  await advanceMaxPrioritization({
    engine, mission: engine.get(mission.id, '10'), tenantId: '10', allowFixtureFallback: true,
  });
  await advanceAcquisitionApproach({
    engine, mission: engine.get(mission.id, '10'), tenantId: '10', allowFixtureFallback: true,
  });
  await advancePaigeVariants({
    engine, mission: engine.get(mission.id, '10'), tenantId: '10', allowFixtureFallback: true,
  });
  await advanceEmmettCapacity({
    engine, mission: engine.get(mission.id, '10'), tenantId: '10', allowFixtureFallback: true,
  });
  return { engine, mission: engine.get(mission.id, '10') };
}

function doctrineCompliantVariants() {
  const copy = buildDefaultColdEmail({
    firstName: 'Sarah',
    companyName: 'Blue Door Living Property Management',
    serviceArea: 'Manchester',
  });
  return {
    variants: [{
      candidateId: 'ChIJ43Z_V2dP4okRCRcDHefV8OU',
      placeId: 'ChIJ43Z_V2dP4okRCRcDHefV8OU',
      companyId: 'ChIJ43Z_V2dP4okRCRcDHefV8OU',
      companyName: 'Blue Door Living Property Management',
      subject: copy.subject,
      body: copy.body,
      cta: copy.cta,
    }],
  };
}

function createOutboundMemoryPool(initialExecutions = []) {
  const executions = new Map(initialExecutions.map((row) => [row.id, row]));
  return {
    query: async (sql, params = []) => {
      const text = String(sql);
      if (/FROM acquisition_mission_outbound_executions/i.test(text) && /mission_id = \$1/i.test(text)) {
        const missionId = params[0];
        const rows = [...executions.values()]
          .filter((row) => row.mission_id === missionId)
          .sort((a, b) => String(b.attempted_at).localeCompare(String(a.attempted_at)));
        return { rows };
      }
      if (/FROM acquisition_missions/i.test(text)) return { rows: [] };
      if (/INSERT INTO acquisition_mission_outbound_executions/i.test(text)) return { rows: [] };
      if (/CREATE TABLE/i.test(text)) return { rows: [] };
      if (text === 'BEGIN' || text === 'COMMIT' || text === 'ROLLBACK') return { rows: [] };
      return { rows: [] };
    },
    connect: async () => ({
      query: async () => ({ rows: [] }),
      release() {},
    }),
    executions,
  };
}

describe('regenerateAnchorPaigeCopyRevision', () => {
  it('1. READY mission regenerates successfully with doctrine-compliant copy', async () => {
    const { engine, mission } = await preparedReadyMission();
    const before = engine.inspect(mission.id, { tenantId: '10' });
    assert.equal(before.mission.stage, STAGES.READY);

    const routed = await routeExecutionRequest(createExecutionRequest({
      source: EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH,
      missionId: mission.id,
      mission: engine.get(mission.id, '10'),
      operatorId: 'operator-1',
      stage: STAGES.READY,
      question: 'Regenerate Paige copy.',
    }), {
      engine,
      tenantId: '10',
      allowFixtureFallback: true,
      runPaige: async () => doctrineCompliantVariants(),
    });

    assert.equal(routed.action, 'revise_prepared_outreach');
    assert.notEqual(routed.executionResult?.rolledBack, true);
    assert.equal(routed.snapshot.mission.stage, STAGES.READY);
    const paige = routed.snapshot.contributions.filter(
      (row) => row.specialist === SPECIALISTS.PAIGE && row.kind === CONTRIBUTION_KINDS.VARIANTS
    ).at(-1);
    const doctrine = validatePaigeVariantsDoctrine(paige.payload);
    assert.equal(doctrine.ok, true, JSON.stringify(doctrine.violations));
  });

  it('2. EXECUTE mission with no sends regenerates and remains in execute', async () => {
    const { engine, mission } = await preparedReadyMission();
    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    const current = engine.get(mission.id, '10');
    applyStageTransition(current, STAGES.EXECUTE, { contributions: snapshot.contributions });
    engine.store.putMission(current);

    const routed = await routeExecutionRequest(createExecutionRequest({
      source: EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH,
      missionId: mission.id,
      mission: engine.get(mission.id, '10'),
      operatorId: 'operator-1',
      stage: STAGES.EXECUTE,
      question: 'Regenerate Paige copy before send.',
    }), {
      engine,
      tenantId: '10',
      allowFixtureFallback: true,
      returnToStage: STAGES.EXECUTE,
      runPaige: async () => doctrineCompliantVariants(),
    });

    assert.equal(routed.action, 'revise_prepared_outreach');
    assert.equal(routed.snapshot.mission.stage, STAGES.EXECUTE);
  });

  it('3. EXECUTE mission with customer send is skipped', async () => {
    const mission = { id: 'mission_sent', stage: STAGES.EXECUTE, tenantId: '10' };
    const contributions = [
      { id: 'p1', missionId: 'mission_sent', specialist: SPECIALISTS.PAIGE, kind: CONTRIBUTION_KINDS.VARIANTS, payload: { variants: [] } },
      { id: 'e1', missionId: 'mission_sent', specialist: SPECIALISTS.EMMETT, kind: CONTRIBUTION_KINDS.CAPACITY, payload: { queue: { items: [] } } },
    ];
    const pool = createOutboundMemoryPool([{
      id: 'exec-1',
      mission_id: 'mission_sent',
      status: 'sent',
      attempted_at: new Date().toISOString(),
    }]);
    const customerSend = await detectCustomerSend('mission_sent', pool);
    const eligibility = classifyMissionEligibility({ mission, contributions, customerSend });
    assert.equal(eligibility.status, 'skip_sent');
    assert.equal(eligibility.reason, 'customer_send_detected');
  });

  it('4. inconsistent approval state repairs when no send exists and copy fails doctrine', async () => {
    const { engine, mission } = await preparedReadyMission();
    const snapshot = engine.inspect(mission.id, { tenantId: '10' });
    const contributions = snapshot.contributions.map((row) => {
      if (row.specialist !== SPECIALISTS.PAIGE || row.kind !== CONTRIBUTION_KINDS.VARIANTS) return row;
      return {
        ...row,
        payload: {
          variants: [{
            candidateId: 'ChIJ43Z_V2dP4okRCRcDHefV8OU',
            subject: 'Cleaning',
            body: 'Hope this finds you well — worth a quick look?',
            cta: 'Worth a quick look?',
          }],
        },
      };
    });
    const broken = {
      ...engine.get(mission.id, '10'),
      pendingOperatorDecision: null,
    };

    const eligibility = classifyMissionEligibility({
      mission: broken,
      contributions,
      customerSend: { detected: false },
    });
    assert.equal(eligibility.status, 'repairable_inconsistent_approval');

    const { seedMissionIntoEngine } = require('../scripts/lib/anchorPaigeCopyRevision');
    seedMissionIntoEngine(engine, { mission: broken, contributions });

    const routed = await routeExecutionRequest(createExecutionRequest({
      source: EXECUTION_SOURCES.API,
      intent: EXECUTION_INTENTS.REVISE_PREPARED_OUTREACH,
      missionId: mission.id,
      mission: engine.get(mission.id, '10'),
      operatorId: 'operator-1',
      stage: STAGES.READY,
      question: 'Repair inconsistent approval with doctrine copy.',
    }), {
      engine,
      tenantId: '10',
      allowFixtureFallback: true,
      runPaige: async () => doctrineCompliantVariants(),
    });

    assert.equal(routed.action, 'revise_prepared_outreach');
    assert.equal(
      routed.snapshot.mission.pendingOperatorDecision?.kind,
      OPERATOR_DECISION_KINDS.EXECUTION_APPROVAL
    );
  });

  it('5. inconsistent approval state skips when send exists', async () => {
    const mission = {
      id: 'mission_inconsistent_sent',
      stage: STAGES.READY,
      tenantId: '10',
      pendingOperatorDecision: null,
    };
    const contributions = [
      { id: 'p1', missionId: mission.id, specialist: SPECIALISTS.PAIGE, kind: CONTRIBUTION_KINDS.VARIANTS, payload: doctrineCompliantVariants() },
      { id: 'e1', missionId: mission.id, specialist: SPECIALISTS.EMMETT, kind: CONTRIBUTION_KINDS.CAPACITY, payload: { queue: { items: [{ email: 'a@example.com', sendable: true }] } } },
    ];
    const pool = createOutboundMemoryPool([{
      id: 'exec-sent',
      mission_id: mission.id,
      status: 'sent',
      attempted_at: new Date().toISOString(),
    }]);
    const customerSend = await detectCustomerSend(mission.id, pool);
    const eligibility = classifyMissionEligibility({ mission, contributions, customerSend });
    assert.equal(eligibility.status, 'skip_sent');
  });

  it('6. one failed mission does not abort the entire batch classification', () => {
    const ready = classifyMissionEligibility({
      mission: { id: 'm1', stage: STAGES.READY },
      contributions: [
        { specialist: SPECIALISTS.PAIGE, kind: CONTRIBUTION_KINDS.VARIANTS, payload: { variants: [{}] } },
        { specialist: SPECIALISTS.EMMETT, kind: CONTRIBUTION_KINDS.CAPACITY, payload: {} },
      ],
      customerSend: { detected: false },
    });
    const wrong = classifyMissionEligibility({
      mission: { id: 'm2', stage: STAGES.PLAN },
      contributions: [],
      customerSend: { detected: false },
    });
    assert.equal(ready.status, 'ready_revision_allowed');
    assert.equal(wrong.status, 'skip_wrong_stage');
  });

  it('7. regenerated copy fails doctrine validation for em dash', () => {
    const result = paigePayloadFailsDoctrine({
      variants: [{
        subject: 'Cleaning',
        body: 'Hi — we handle office cleaning.',
        cta: 'Reply if useful',
      }],
    });
    assert.equal(result.fails, true);
    assert.ok(result.violations[0].violations.some((v) => v.patternId === 'em_dash'));
  });

  it('8. regenerated copy fails doctrine validation for generic closer', () => {
    const result = validatePaigeVariantsDoctrine({
      variants: [{
        subject: 'Cleaning for Example Co',
        body: 'We handle office cleaning in Manchester.',
        cta: 'Worth a quick look?',
      }],
    });
    assert.equal(result.ok, false);
    assert.ok(result.violations[0].violations.some((v) => v.patternId === 'worth_quick_look'));
  });

  it('9. regenerated copy fails doctrine validation for unsupported observation', () => {
    const result = validateAnchorCopyDoctrine({
      subject: 'Cleaning',
      body: 'I saw your shared entrance gets heavy foot traffic.',
      cta: 'Reply if useful',
    });
    assert.equal(result.ok, false);
    assert.ok(result.violations.some((v) => v.patternId === 'i_saw_observation'));
  });

  it('10. script summary reports all statuses', async () => {
    const missions = [
      {
        mission: { id: 'm-ready', stage: STAGES.READY, tenantId: '10' },
        contributions: [
          { specialist: SPECIALISTS.PAIGE, kind: CONTRIBUTION_KINDS.VARIANTS, payload: { variants: [{}] } },
          { specialist: SPECIALISTS.EMMETT, kind: CONTRIBUTION_KINDS.CAPACITY, payload: {} },
        ],
      },
      {
        mission: { id: 'm-sent', stage: STAGES.EXECUTE, tenantId: '10' },
        contributions: [
          { specialist: SPECIALISTS.PAIGE, kind: CONTRIBUTION_KINDS.VARIANTS, payload: { variants: [{}] } },
          { specialist: SPECIALISTS.EMMETT, kind: CONTRIBUTION_KINDS.CAPACITY, payload: {} },
        ],
      },
      {
        mission: { id: 'm-plan', stage: STAGES.PLAN, tenantId: '10' },
        contributions: [],
      },
    ];

    const summary = {
      ready_revised: 0,
      execute_revised_before_send: 0,
      approval_repaired: 0,
      skipped_sent: 0,
      skipped_wrong_stage: 0,
      skipped_inconsistent: 0,
      failed: 0,
    };
    const results = [];
    const pool = createOutboundMemoryPool([{
      id: 'sent-1',
      mission_id: 'm-sent',
      status: 'sent',
      attempted_at: new Date().toISOString(),
    }]);

    for (const candidate of missions) {
      const customerSend = await detectCustomerSend(candidate.mission.id, pool);
      const eligibility = classifyMissionEligibility({
        mission: candidate.mission,
        contributions: candidate.contributions,
        customerSend,
      });
      results.push({ mission_id: candidate.mission.id, status: eligibility.status });
      if (eligibility.status === 'ready_revision_allowed') summary.ready_revised += 1;
      else if (eligibility.status === 'skip_sent') summary.skipped_sent += 1;
      else if (eligibility.status === 'skip_wrong_stage') summary.skipped_wrong_stage += 1;
    }

    assert.equal(summary.ready_revised, 1);
    assert.equal(summary.skipped_sent, 1);
    assert.equal(summary.skipped_wrong_stage, 1);
    assert.equal(results.length, 3);
  });

  it('parseArgs accepts --confirm-production and optional --mission-id', () => {
    const opts = parseArgs(['--confirm-production', '--mission-id', 'mission_abc']);
    assert.equal(opts.confirmProduction, true);
    assert.equal(opts.missionId, 'mission_abc');
  });

  it('assertRuntimeEnv fails closed without DATABASE_URL', () => {
    const prev = process.env.DATABASE_URL;
    delete process.env.DATABASE_URL;
    assert.throws(() => assertRuntimeEnv(), (err) => err.code === 'runtime_env_missing');
    process.env.DATABASE_URL = prev;
  });

  it('run refuses without --confirm-production', async () => {
    await assert.rejects(
      () => run({ confirmProduction: false }),
      (err) => err.code === 'confirm_production_required'
    );
  });
});
