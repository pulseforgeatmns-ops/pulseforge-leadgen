'use strict';

/**
 * PILOT-0 AUDIT-001 — reasoning pipeline conformance.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const {
  bindGovernedReasoning,
  COMPOSER_ID,
  PIPELINE_ID,
} = require('../ReasoningPipeline');
const { CONTRACT_IDS, selectResponseContract } = require('../ResponseContract');
const { OPERATOR_INTENTS } = require('../OperatorIntentRegistry');
const { classifyCognitiveMode, COGNITIVE_MODES } = require('../../specialistDelegation/CognitiveMode');
const { maybeHandleRetrievalBeforeDelegationTurn } = require('../RetrievalBeforeDelegationContext');
const { shouldClaimClientIntelligenceTurn } = require('../ClientIntelligenceContext');
const { shouldHandleScoutAcquisition } = require('../ScoutAcquisitionContext');
const { createWorkspaceEngine } = require('../WorkspaceEngine');
const training = require('../../training');

const PROMPTS = [
  ['How is Anchor doing?', CONTRACT_IDS.SUMMARY, OPERATOR_INTENTS.SUMMARY],
  ['What should I do next?', CONTRACT_IDS.RECOMMENDATION, OPERATOR_INTENTS.RECOMMENDATION],
  ["What's preventing growth?", CONTRACT_IDS.DIAGNOSIS, OPERATOR_INTENTS.DIAGNOSIS],
  ["What don't we know?", CONTRACT_IDS.UNKNOWN_ANALYSIS, OPERATOR_INTENTS.UNKNOWN_ANALYSIS],
  ["What's risky?", CONTRACT_IDS.RISK, OPERATOR_INTENTS.RISK],
  ['What outreach has been sent?', CONTRACT_IDS.RETRIEVAL, OPERATOR_INTENTS.RETRIEVAL],
  ['Should Scout investigate?', CONTRACT_IDS.INVESTIGATION, OPERATOR_INTENTS.INVESTIGATION],
];

const BLUEPRINT_ADVISORY_RE =
  /I'd start by proving a repeatable commercial acquisition motion/i;

function operatingOpts() {
  return {
    now: new Date('2026-08-17T15:00:00.000Z'),
    loadCampaignAo: async () => ({
      available: true,
      campaignName: 'Campaign 001',
      mailExecuted: true,
      progress: {
        campaign_name: 'Campaign 001',
        target_total: 20,
        seeded_in_ao: 20,
        visited: 0,
        walkthrough_requests: 0,
        remaining_route_queue: 20,
      },
      leads: Array.from({ length: 20 }, (_, i) => ({
        id: i + 1,
        client_id: 10,
        campaign_name: 'Campaign 001',
        operational_state: 'not_started',
        mail_status: 'mailed',
      })),
    }),
    loadProspects: async () => ({
      available: true,
      counts: { total: 72, qualified: 54, cold: 40, warm: 10, hot: 4 },
    }),
    loadScout: async () => ({
      available: true,
      launchedNewWork: false,
      intelligence: {
        counts: { considered: 69, matched: 69 },
        companies: Array.from({ length: 8 }, (_, i) => ({
          id: `co-${i + 1}`,
          tenantId: '10',
          name: `Company ${i + 1}`,
        })),
      },
      state: { tenantId: '10', opportunityCount: 69 },
    }),
    loadMissions: async () => ({ available: true, rows: [] }),
    loadObjectives: async () => ({
      available: true,
      rows: [{ id: 'obj-1', clientId: 10, title: 'Grow commercial cleaning', status: 'active' }],
    }),
    loadActivity: async () => ({
      available: true,
      touchpoints: [{ id: 1, client_id: 10, channel: 'mail', action_type: 'delivery_log' }],
      activity: [],
    }),
    loadOutcomes: async () => ({ available: true, jobs: 0, payments: 0 }),
    loadOperatorAttested: async () => ({
      available: true,
      claims: [
        {
          status: 'active',
          statement: 'Campaign 001 was physically mailed on August 6.',
          metadata: { operatingUpdate: true, predicate: 'physical_mail_execution', occurredAt: '2026-08-06' },
        },
      ],
    }),
    loadCapability: async () => ({
      available: true,
      enabled_agents: ['scout'],
      autosend_enabled: false,
    }),
  };
}

function sessionContext() {
  return {
    tenantId: '10',
    clientId: 10,
    clientIntelligence: {
      approved: true,
      businessName: 'Anchor Cleaning',
      identity: 'Anchor Cleaning — commercial cleaning for professional offices.',
      geography: 'Greater Manchester including Bedford and Hooksett',
      idealCustomers: 'property managers and professional offices',
      goals: 'Grow commercial cleaning in Greater Manchester.',
      unknowns: ['Which commercial segment will respond first'],
    },
  };
}

describe('AUDIT-001 analysis mode table', () => {
  for (const [prompt, contractId, intent] of PROMPTS) {
    it(`classifies "${prompt}" as ${intent}`, () => {
      const bound = bindGovernedReasoning(prompt, { session: { context: sessionContext() } });
      assert.equal(bound.governed, true, prompt);
      assert.equal(bound.composer, COMPOSER_ID, prompt);
      assert.equal(bound.contract.id, contractId, prompt);
      assert.equal(bound.analysis.intent, intent, prompt);
      assert.equal(bound.analysis.analysisMode, intent, prompt);
      assert.doesNotMatch(prompt, BLUEPRINT_ADVISORY_RE);
    });
  }

  it('unknown intent fails toward Retrieval, never Recommendation', () => {
    const bound = bindGovernedReasoning('hmm interesting', {
      session: { context: sessionContext() },
    });
    assert.equal(bound.analysis.kind, COGNITIVE_MODES.RETRIEVAL);
    assert.equal(bound.analysis.fallbackUsed, true);
    assert.equal(bound.contract.id, CONTRACT_IDS.RETRIEVAL);
    assert.notEqual(bound.contract.id, CONTRACT_IDS.RECOMMENDATION);
  });
});

describe('AUDIT-001 no Blueprint Advisory responder', () => {
  for (const [prompt] of PROMPTS) {
    it(`does not send "${prompt}" to CIE advisory or Scout shortcut`, () => {
      assert.equal(
        shouldClaimClientIntelligenceTurn(prompt, null, { approvedBlueprint: true }),
        false,
        prompt
      );
      if (prompt !== 'Should Scout investigate?') {
        assert.equal(
          shouldHandleScoutAcquisition({ question: prompt, context: { tenantId: '10' } }),
          false,
          prompt
        );
      }
    });
  }

  it('business-framed next-action does not invoke Blueprint Advisory', async () => {
    const context = sessionContext();
    const question =
      'Based on what you know about my business, what should we focus on first?';
    const bound = bindGovernedReasoning(question, { session: { context } });
    assert.equal(bound.contract.id, CONTRACT_IDS.RECOMMENDATION);
    const turn = await maybeHandleRetrievalBeforeDelegationTurn({
      question,
      session: { id: 'audit-001', context },
      context,
      cognitive: bound.analysis,
      responseContract: bound.contract,
      operatingEvidenceOpts: operatingOpts(),
    });
    assert.ok(turn);
    assert.equal(turn.responseContract.id, CONTRACT_IDS.RECOMMENDATION);
    assert.doesNotMatch(turn.prose, BLUEPRINT_ADVISORY_RE);
    assert.ok(turn.structured.metadata.pipelineLog);
    assert.equal(turn.structured.metadata.pipelineLog.composer, COMPOSER_ID);
    assert.equal(turn.structured.metadata.pipelineLog.responseContract, CONTRACT_IDS.RECOMMENDATION);
  });
});

describe('AUDIT-001 pipeline log', () => {
  it('exposes intent, analysis mode, contract, evidence, claims, BI, composer', async () => {
    const context = sessionContext();
    const question = "What's preventing growth?";
    const bound = bindGovernedReasoning(question, { session: { context } });
    const turn = await maybeHandleRetrievalBeforeDelegationTurn({
      question,
      session: { id: 'audit-001-log', context },
      context,
      cognitive: bound.analysis,
      responseContract: bound.contract,
      operatingEvidenceOpts: operatingOpts(),
    });
    const log = turn.structured.metadata.pipelineLog;
    assert.equal(log.pipelineId, PIPELINE_ID);
    assert.equal(log.intent, OPERATOR_INTENTS.DIAGNOSIS);
    assert.equal(log.analysisMode, OPERATOR_INTENTS.DIAGNOSIS);
    assert.equal(log.responseContract, CONTRACT_IDS.DIAGNOSIS);
    assert.equal(typeof log.evidenceCount, 'number');
    assert.equal(typeof log.groundedClaims, 'number');
    assert.equal(typeof log.businessIntelligenceObjects, 'number');
    assert.ok(Array.isArray(log.reasoningComponents));
    assert.equal(log.composer, COMPOSER_ID);
  });
});

describe('AUDIT-001 engine.ask conformance', () => {
  it('WorkspaceEngine is the single reasoning entry and never emits Blueprint Advisory', async () => {
    const engine = createWorkspaceEngine({
      disableLlm: true,
      missionsEnabled: false,
      operatingEvidenceOpts: operatingOpts(),
    });
    const opened = engine.open({
      tenantId: '10',
      page: 'command-deck',
      clientIntelligence: sessionContext().clientIntelligence,
    });
    const result = await engine.ask({
      sessionId: opened.sessionId,
      question: 'What should I do next?',
    });
    assert.doesNotMatch(result.prose, BLUEPRINT_ADVISORY_RE);
    assert.equal(result.structured.metadata.composer, COMPOSER_ID);
    assert.equal(result.structured.metadata.responseContract, CONTRACT_IDS.RECOMMENDATION);
    assert.equal(result.structured.metadata.pipelineLog.analysisMode, OPERATOR_INTENTS.RECOMMENDATION);
  });
});

describe('AUDIT-001 registry and spec', () => {
  it('registers reasoning_pipeline_conformance as a graduated competency', () => {
    const competency = training.getCompetency('reasoning_pipeline_conformance');
    assert.ok(competency);
    assert.equal(competency.stage, training.STAGES.GRADUATED);
    assert.ok(competency.specRefs.includes('SPEC-112'));
  });

  it('keeps SPEC-112 and ADR-049 on disk', () => {
    const spec = path.join(__dirname, '../../../../docs/specs/SPEC-112_Reasoning_Pipeline_Conformance.md');
    const adr = path.join(__dirname, '../../../../docs/adr/ADR-049_Single_Governed_Reasoning_Pipeline.md');
    assert.equal(fs.existsSync(spec), true);
    assert.equal(fs.existsSync(adr), true);
  });

  it('selectResponseContract never returns a Blueprint advisory contract', () => {
    for (const [prompt] of PROMPTS) {
      const mode = classifyCognitiveMode(prompt);
      const contract = selectResponseContract(prompt, mode);
      assert.ok(contract);
      assert.notEqual(contract.id, 'blueprint_advisory');
    }
  });
});
