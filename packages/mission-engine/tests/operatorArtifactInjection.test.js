'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  createMissionEngine,
  createArtifactBus,
  OperatorArtifactInjection,
  STAGE_OUTCOMES,
  AUDIT_KINDS,
  MISSION_STATUS,
  ARTIFACT_VALIDATION_STATUS,
  discoveryRecoveryActions,
} = require('..');
const {
  createBuiltinRegistry,
  BUILTIN_IDS,
} = require('../../capabilities');

const {
  parseDelimitedProspects,
  validateOperatorProspectRows,
  publishOperatorProspectList,
  detectOperatorProspectListInMessage,
  OPERATOR_PRODUCERS,
  OPERATOR_SOURCES,
} = OperatorArtifactInjection;

function testEngine() {
  return createMissionEngine({
    registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
  });
}

async function blockedDiscoveryMission(engine) {
  let mission = await engine.createFromObjective({
    objective: 'Build Campaign 001 for Anchor Cleaning',
    tenantId: 10,
    clientId: 10,
    execute: false,
    constraints: { targetCount: 5 },
  });
  const steps = ((mission.plan && mission.plan.steps) || []).map((s) =>
    s.stageId === 'prospect_discovery' ||
    s.capabilityId === BUILTIN_IDS.PROSPECT_DISCOVERY
      ? {
          ...s,
          status: 'blocked',
          outcome: STAGE_OUTCOMES.BLOCKED,
          blockingIssues: [
            'Discovery returned zero verified companies. Campaign generation cannot continue.',
          ],
        }
      : s
  );
  mission = await engine.store.update({
    id: mission.id,
    status: MISSION_STATUS.WAITING,
    plan: { ...mission.plan, steps },
    blockingIssues: [
      'Discovery returned zero verified companies. Campaign generation cannot continue.',
    ],
    stageReview: {
      capabilityId: BUILTIN_IDS.PROSPECT_DISCOVERY,
      outcome: STAGE_OUTCOMES.BLOCKED,
      blockingIssues: [
        'Discovery returned zero verified companies. Campaign generation cannot continue.',
      ],
    },
    deliverables: {
      artifactBus: { version: 1, artifacts: [], events: [] },
      stepResults: [],
    },
    progress: {
      ...(mission.progress || {}),
      currentStage: 'Blocked — Discovery returned zero verified companies',
      stageOutcome: STAGE_OUTCOMES.BLOCKED,
    },
  });
  return mission;
}

describe('SPEC-043 OperatorArtifactInjection normalize/validate', () => {
  it('parses CSV with headers into prospect rows', () => {
    const rows = parseDelimitedProspects(
      'Company Name,Website,Address\nAcme Law,https://acme.example,1 Main St\nBeta CPA,,2 Oak'
    );
    assert.equal(rows.length, 2);
    assert.equal(rows[0].companyName, 'Acme Law');
    assert.equal(rows[0].website, 'https://acme.example');
    assert.equal(rows[1].companyName, 'Beta CPA');
    assert.equal(rows[1].website, null);
  });

  it('preserves all rows for CR-only and headerless company-name lists', () => {
    const crOnly = parseDelimitedProspects(
      'Company Name,Website\rAcme Law,https://a.example\rBeta CPA,https://b.example\rGamma LLC,https://c.example'
    );
    assert.equal(crOnly.length, 3);
    assert.deepEqual(
      crOnly.map((r) => r.companyName),
      ['Acme Law', 'Beta CPA', 'Gamma LLC']
    );

    const nameList = parseDelimitedProspects(
      'Acme Law, Beta CPA, Gamma LLC, Delta Partners'
    );
    assert.equal(nameList.length, 4);
    assert.equal(nameList[0].companyName, 'Acme Law');
    assert.equal(nameList[3].companyName, 'Delta Partners');
    assert.equal(nameList[0].website, null);
  });

  it('requires company name and warns on missing recommended fields', () => {
    const result = validateOperatorProspectRows([
      { companyName: 'Acme' },
      { website: 'https://x.example' },
    ]);
    assert.equal(result.ok, false);
    assert.ok(result.errors.some((e) => /Company Name is required/i.test(e)));
    assert.equal(result.prospects.length, 1);
    assert.ok(result.warnings.some((w) => /Website recommended/i.test(w)));
  });

  it('publishes consumable ProspectList with operator provenance', () => {
    const bus = createArtifactBus();
    const published = publishOperatorProspectList({
      bus,
      missionId: 'msn_op',
      csv: 'Company Name,Website,Address\nAcme Law,https://acme.example,1 Main St',
      createdBy: 'op@example.com',
    });
    assert.equal(published.ok, true);
    assert.equal(published.producer, OPERATOR_PRODUCERS.IMPORT);
    assert.equal(published.source, OPERATOR_SOURCES.CSV_IMPORT);
    assert.ok(
      [
        ARTIFACT_VALIDATION_STATUS.VALID,
        ARTIFACT_VALIDATION_STATUS.VALID_WITH_WARNINGS,
      ].includes(published.artifact.validationStatus)
    );
    assert.equal(
      published.artifact.metadata.provenance.createdBy,
      'op@example.com'
    );
    const latest = bus.getLatestArtifact('msn_op', 'ProspectList');
    assert.equal(latest.id, published.artifact.id);
    assert.equal(latest.producer, OPERATOR_PRODUCERS.IMPORT);
  });
});


describe('SPEC-043 MissionEngine injectProspectList', () => {
  it('surfaces Import Prospect List recovery when Discovery is blocked', async () => {
    const engine = testEngine();
    const mission = await blockedDiscoveryMission(engine);
    const workspace = await engine.getWorkspace(mission.id);
    assert.ok(
      workspace.recoveryActions.some((a) => a.id === 'import_prospect_list')
    );
    assert.ok(
      discoveryRecoveryActions(mission).some((a) => a.id === 'retry_discovery')
    );
  });

  it('marks Discovery Satisfied (Operator Supplied) and resumes Mission', async () => {
    const engine = testEngine();
    const blocked = await blockedDiscoveryMission(engine);
    const result = await engine.injectProspectList({
      missionId: blocked.id,
      csv:
        'Company Name,Website,Address\n' +
        'Granite State Law,https://gslaw.example,100 Elm St Manchester NH\n' +
        'Queen City CPA,https://qcpa.example,200 Bridge St Manchester NH',
      createdBy: 'tester@gopulseforge.com',
    });

    assert.equal(result.executed, true);
    assert.ok(result.artifact);
    assert.equal(result.artifact.producer, OPERATOR_PRODUCERS.IMPORT);

    const discovery = result.mission.plan.steps.find(
      (s) =>
        s.stageId === 'prospect_discovery' ||
        s.capabilityId === BUILTIN_IDS.PROSPECT_DISCOVERY
    );
    assert.ok(discovery);
    assert.equal(discovery.status, 'completed');
    assert.equal(
      discovery.outcome,
      STAGE_OUTCOMES.SATISFIED_OPERATOR_SUPPLIED
    );
    assert.match(String(discovery.outcomeLabel || ''), /Operator Supplied/i);

    const bus = createArtifactBus({
      snapshot: result.mission.deliverables.artifactBus,
    });
    const list = bus.getLatestArtifact(blocked.id, 'ProspectList');
    assert.ok(list);
    assert.equal(list.producer, OPERATOR_PRODUCERS.IMPORT);
    assert.equal(list.payload.prospectCount, 2);
    assert.equal(list.payload.prospects.length, 2);
    assert.deepEqual(
      list.payload.prospects.map((p) => p.companyName),
      ['Granite State Law', 'Queen City CPA']
    );

    // Company Intelligence consumes by type/status — origin is provenance only
    const enrichment = result.mission.plan.steps.find(
      (s) =>
        s.stageId === 'company_enrichment' ||
        s.capabilityId === BUILTIN_IDS.COMPANY_ENRICHMENT
    );
    assert.ok(enrichment);
    assert.notEqual(enrichment.status, 'blocked');
    assert.notEqual(enrichment.status, 'queued');

    const intelligence = bus.getLatestArtifact(blocked.id, 'CompanyIntelligence');
    assert.ok(intelligence, 'Company Intelligence should publish for every imported prospect');
    assert.equal(intelligence.payload.enrichedCount, 2);
    assert.equal(intelligence.payload.prospects.length, 2);
    assert.deepEqual(
      intelligence.payload.prospects.map((p) => p.companyName),
      ['Granite State Law', 'Queen City CPA']
    );

    const discoveryStep = (result.mission.deliverables.stepResults || []).find(
      (s) => s.capabilityId === BUILTIN_IDS.PROSPECT_DISCOVERY
    );
    assert.ok(discoveryStep, 'Operator Discovery stepResult must survive execute');
    assert.equal(
      discoveryStep.outputs && discoveryStep.outputs.prospectCount,
      2
    );

    const audit = await engine.listAudit(blocked.id);
    assert.ok(audit.some((a) => a.kind === AUDIT_KINDS.ARTIFACT_INJECTED));
    assert.ok(audit.some((a) => a.kind === AUDIT_KINDS.STAGE_SATISFIED));

    assert.ok(
      [MISSION_STATUS.REVIEW_REQUIRED, MISSION_STATUS.WAITING].includes(
        result.mission.status
      ) || result.mission.status === MISSION_STATUS.COMPLETED
    );
  });

  it('rejects invalid imports without publishing a consumable list', async () => {
    const engine = testEngine();
    const blocked = await blockedDiscoveryMission(engine);
    await assert.rejects(
      () =>
        engine.injectProspectList({
          missionId: blocked.id,
          prospects: [{ website: 'https://no-name.example' }],
          execute: false,
        }),
      /Company Name|validation failed/i
    );
    const mission = await engine.get(blocked.id);
    const bus = createArtifactBus({
      snapshot:
        (mission.deliverables && mission.deliverables.artifactBus) || null,
    });
    assert.equal(bus.getLatestArtifact(blocked.id, 'ProspectList'), null);
  });

  it('keeps Discovery fixture path working (regression)', async () => {
    const engine = testEngine();
    const mission = await engine.createFromObjective({
      objective: 'Build Campaign 001 for Anchor Cleaning',
      tenantId: 10,
      clientId: 10,
      constraints: { targetCount: 5 },
    });
    assert.ok(mission.deliverables && mission.deliverables.artifactBus);
    const bus = createArtifactBus({
      snapshot: mission.deliverables.artifactBus,
    });
    const list = bus.getLatestArtifact(mission.id, 'ProspectList');
    assert.ok(list, 'Discovery should still publish ProspectList');
    assert.ok(list.payload.prospectCount > 0);
    const history = bus.getArtifactHistory(mission.id, 'ProspectList');
    assert.ok(
      history.some((a) => a.producer === BUILTIN_IDS.PROSPECT_DISCOVERY),
      'Discovery remains a ProspectList producer'
    );
  });
});

describe('SPEC-043 chat prompt ProspectList detection', () => {
  it('detects CSV pasted after Build Campaign objective', () => {
    const detected = detectOperatorProspectListInMessage(
      [
        'Build Campaign 001 for Anchor Cleaning',
        '',
        'Company Name,Website,Address',
        'Granite State Law,https://gslaw.example,100 Elm St',
        'Queen City CPA,https://qcpa.example,200 Bridge St',
      ].join('\n')
    );
    assert.equal(detected.detected, true);
    assert.equal(detected.confidence, 'high');
    assert.equal(detected.autoInject, true);
    assert.equal(detected.prospectCount, 2);
    assert.match(detected.objectiveText, /Build Campaign 001/i);
  });

  it('auto-injects on createFromObjective and skips Discovery producer', async () => {
    const engine = testEngine();
    const mission = await engine.createFromObjective({
      objective: [
        'Build Campaign 001 for Anchor Cleaning',
        '',
        'Company Name,Website,Address',
        'Granite State Law,https://gslaw.example,100 Elm St Manchester NH',
        'Queen City CPA,https://qcpa.example,200 Bridge St Manchester NH',
      ].join('\n'),
      tenantId: 10,
      clientId: 10,
    });

    assert.ok(mission.operatorProspectList);
    assert.equal(mission.operatorProspectList.injected, true);
    assert.equal(mission.operatorProspectList.prospectCount, 2);

    // SPEC-051: Discovery is not on the plan when ProspectList was resolved
    const discovery = mission.plan.steps.find(
      (s) =>
        s.stageId === 'prospect_discovery' ||
        s.capabilityId === BUILTIN_IDS.PROSPECT_DISCOVERY
    );
    assert.equal(discovery, undefined);
    assert.ok(
      mission.plan.artifactResolution &&
        mission.plan.artifactResolution.resolved.some(
          (r) => r.type === 'ProspectList'
        )
    );
    assert.match(
      mission.plan.skippedStages.prospect_discovery || '',
      /Compatible ProspectList/i
    );

    const bus = createArtifactBus({
      snapshot: mission.deliverables.artifactBus,
    });
    const list = bus.getLatestArtifact(mission.id, 'ProspectList');
    assert.ok(list);
    assert.equal(list.producer, OPERATOR_PRODUCERS.IMPORT);
    assert.equal(list.payload.prospectCount, 2);

    const history = bus.getArtifactHistory(mission.id, 'ProspectList');
    assert.ok(
      !history.some((a) => a.producer === BUILTIN_IDS.PROSPECT_DISCOVERY),
      'Discovery should not have published when operator list was injected'
    );
  });

  it('prompts import when list-like content fails validation', async () => {
    const engine = testEngine();
    const mission = await engine.createFromObjective({
      objective: [
        'Build Campaign 001 for Anchor Cleaning',
        '',
        'Company Name,Website',
        ',https://missing-name.example',
        ',https://also-missing.example',
      ].join('\n'),
      tenantId: 10,
      clientId: 10,
      execute: false,
    });

    assert.ok(mission.operatorProspectList);
    assert.equal(mission.operatorProspectList.promptImport, true);
    assert.equal(mission.operatorProspectList.autoInject, false);
    assert.ok(
      mission.deliverables && mission.deliverables.pendingOperatorImport
    );
    assert.ok(mission.deliverables.pendingOperatorImport.paste);
  });
});

const CANARY_PROSPECT_PROMPT = [
  'Use these 3 prospects for the Campaign 001 preparation-only canary package:',
  '',
  '1. PM-001 — Gamache Properties — Ben Gamache — Property Management',
  '2. PM-002 — Elm Grove Companies — David Schleyer — Property Management',
  '3. PM-003 — Mill City Property Management — Lauren DuPaul — Property Management',
  '',
  'Still do not launch, execute, approve, or mail anything.',
  '',
  'Prepare the review package only. For each prospect, return:',
  '- readiness status',
  '- missing or unverified fields',
  '- packet checklist',
  '- personalized letter',
  '- handwritten note',
  '- scorecard cover text',
  '- first follow-up call notes',
  '- next action',
  '- tracking fields',
].join('\n');

describe('Campaign 001 canary prospect block extraction', () => {
  it('extracts numbered canary prospect rows and keeps instructions as objective', () => {
    const detected = detectOperatorProspectListInMessage(CANARY_PROSPECT_PROMPT);
    assert.equal(detected.detected, true);
    assert.equal(detected.prospectCount, 3);
    assert.deepEqual(
      detected.prospects.map((p) => p.companyName),
      [
        'Gamache Properties',
        'Elm Grove Companies',
        'Mill City Property Management',
      ]
    );
    assert.deepEqual(
      detected.prospects.map((p) => p.id),
      ['PM-001', 'PM-002', 'PM-003']
    );
    assert.deepEqual(
      detected.prospects.map((p) => p.contactName),
      ['Ben Gamache', 'David Schleyer', 'Lauren DuPaul']
    );
    assert.ok(
      detected.prospects.every(
        (p) =>
          !/Still do not launch/i.test(p.companyName) &&
          !/Prepare the review package/i.test(p.companyName)
      )
    );
    assert.match(detected.objectiveText, /Still do not launch/i);
    assert.match(detected.objectiveText, /Prepare the review package only/i);
  });

  it('parses tab-separated canary rows without treating instruction lines as prospects', () => {
    const detected = detectOperatorProspectListInMessage(
      [
        'Use these 3 prospects for the Campaign 001 preparation-only canary package:',
        '',
        'PM-001\tGamache Properties\tBen Gamache\tProperty Management',
        'PM-002\tElm Grove Companies\tDavid Schleyer\tProperty Management',
        'PM-003\tMill City Property Management\tLauren DuPaul\tProperty Management',
        '',
        'Still do not launch, execute, approve, or mail anything.',
      ].join('\n')
    );
    assert.equal(detected.detected, true);
    assert.equal(detected.prospectCount, 3);
    assert.equal(detected.prospects[0].companyName, 'Gamache Properties');
    assert.doesNotMatch(
      detected.prospects.map((p) => p.companyName).join('|'),
      /Still do not launch/
    );
    assert.match(detected.objectiveText, /Still do not launch/i);
  });

  it('extracts exactly three prospects from a flattened numbered paragraph', () => {
    const detected = detectOperatorProspectListInMessage(
      [
        'Continue the Campaign 001 preparation-only canary package.',
        'Use these 3 prospects: 1. PM-001 — Gamache Properties — Ben Gamache — Property Management — website unknown — mailing address unknown — phone unknown 2. PM-002 — Elm Grove Companies — David Schleyer — Property Management — website unknown — mailing address unknown — phone unknown 3. PM-003 — Mill City Property Management — Lauren DuPaul — Property Management — website unknown — mailing address unknown — phone unknown Do not create a mission. Do not launch, execute, approve, print, or mail anything.',
      ].join('\n')
    );
    assert.equal(detected.detected, true);
    assert.equal(detected.prospectCount, 3);
    assert.deepEqual(
      detected.prospects.map((p) => p.id),
      ['PM-001', 'PM-002', 'PM-003']
    );
    assert.deepEqual(
      detected.prospects.map((p) => p.companyName),
      [
        'Gamache Properties',
        'Elm Grove Companies',
        'Mill City Property Management',
      ]
    );
    assert.deepEqual(
      detected.prospects.map((p) => p.contactName),
      ['Ben Gamache', 'David Schleyer', 'Lauren DuPaul']
    );
    assert.ok(
      detected.prospects.every((p) => p.industry === 'Property Management')
    );
    assert.ok(
      detected.prospects.every(
        (p) =>
          !/Do not create/i.test(p.companyName) &&
          !/Do not launch/i.test(p.companyName)
      )
    );
    assert.match(detected.objectiveText, /Do not create a mission/i);
  });

  it('does not sniff fillable table field assignments as a ProspectList', () => {
    const prompt = [
      'Update the fillable verification table.',
      'For PM-001 only, set:',
      '- website_status = verified',
      '- website_value = https://www.gamacheproperties.com',
      '- phone_status = verified',
      '- phone_value = 603-555-0198',
      '- mailing_address_status = verified',
      '- mailing_address_value = 100 Market St, Manchester NH',
      '- notes = mail gates verified; contact role still needs verification',
      '',
      'Leave PM-002 and PM-003 unchanged.',
      'Reassess PM-001 readiness using the table gates.',
      'Return only the updated table plus one short preparation-only safety line.',
    ].join('\n');

    const detected = detectOperatorProspectListInMessage(prompt);
    assert.equal(detected.detected, false);
    assert.equal(detected.autoInject, false);
    assert.equal(detected.promptImport, false);
    assert.equal(detected.prospectCount, 0);
    assert.equal(detected.suppressedFillableTableUpdate, true);
    assert.ok(!(detected.prospects || []).some((p) => /For PM-001 only/i.test(p.companyName || '')));
    assert.ok(!(detected.prospects || []).some((p) => /^set:?$/i.test(p.companyName || '')));
  });

  it('does not sniff canary table reassessment as a ProspectList', () => {
    const prompt = [
      'Update the fillable verification table.',
      'For PM-001 only, set:',
      '- contact_role_status = verified',
      '',
      'Leave PM-002 and PM-003 unchanged.',
      'Reassess the Campaign 001 canary table.',
    ].join('\n');

    const detected = detectOperatorProspectListInMessage(prompt);
    assert.equal(detected.detected, false);
    assert.equal(detected.autoInject, false);
    assert.equal(detected.promptImport, false);
    assert.equal(detected.prospectCount, 0);
    assert.equal(detected.suppressedFillableTableUpdate, true);
  });
});
