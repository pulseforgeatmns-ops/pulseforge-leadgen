'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  createMissionEngine,
  createArtifactBus,
  ARTIFACT_VALIDATION_STATUS,
  validateArtifactCandidate,
  looksLikeNaturalLanguage,
  isViableCompanyName,
  detectOperatorProspectListInMessage,
} = require('..');
const { createBuiltinRegistry } = require('../../capabilities');
const { ARTIFACT_TYPES, SCHEMA_VERSION } = require('../ArtifactRegistry');

function testEngine() {
  return createMissionEngine({
    registry: createBuiltinRegistry({ discovery: { useFixture: true } }),
  });
}

describe('SPEC-052 ArtifactValidator', () => {
  it('rejects unknown artifact types', () => {
    const result = validateArtifactCandidate({
      type: 'NotARealType',
      payload: {},
    });
    assert.equal(result.ok, false);
    assert.match(result.errors[0], /Unknown artifact type/i);
    assert.equal(result.remainsPlainText, true);
  });

  it('rejects plain-text payloads', () => {
    const result = validateArtifactCandidate({
      artifactType: 'ProspectList',
      payload: 'Build Campaign 001 and reuse existing ProspectList',
    });
    assert.equal(result.ok, false);
    assert.equal(result.remainsPlainText, true);
  });

  it('rejects ProspectList natural-language company rows', () => {
    const result = validateArtifactCandidate({
      artifactType: ARTIFACT_TYPES.PROSPECT_LIST,
      payload: {
        prospects: [
          { companyName: 'Reuse existing ProspectList' },
          { companyName: 'Execute the complete pipeline' },
        ],
        prospectCount: 2,
      },
    });
    assert.equal(result.ok, false);
    assert.ok(result.errors.some((e) => /natural language/i.test(e)));
    assert.ok(result.errors.some((e) => /No valid prospect/i.test(e)));
    assert.ok(result.review);
    assert.equal(result.review.status, 'FAILED');
    assert.equal(result.review.artifactType, 'ProspectList');
  });

  it('accepts ProspectList with viable company names', () => {
    const result = validateArtifactCandidate({
      artifactType: 'ProspectList',
      payload: {
        prospects: [
          { companyName: 'Granite State Law', website: 'https://gslaw.example' },
          { companyName: 'Queen City CPA LLC' },
        ],
        prospectCount: 2,
      },
    });
    assert.equal(result.ok, true);
    assert.ok(
      [
        ARTIFACT_VALIDATION_STATUS.VALID,
        ARTIFACT_VALIDATION_STATUS.VALID_WITH_WARNINGS,
      ].includes(result.status)
    );
  });

  it('rejects incompatible schema major version', () => {
    const result = validateArtifactCandidate({
      artifactType: 'ProspectList',
      schemaVersion: '9.0.0',
      payload: {
        prospects: [{ companyName: 'Acme Law' }],
        prospectCount: 1,
      },
    });
    assert.equal(result.ok, false);
    assert.ok(result.errors.some((e) => /Incompatible schema version/i.test(e)));
  });

  it('rejects Campaign without prospects', () => {
    const result = validateArtifactCandidate({
      artifactType: 'Campaign',
      payload: { campaign: { name: 'Empty', prospectCount: 0 } },
    });
    assert.equal(result.ok, false);
  });

  it('rejects SalesIntelligence without reasoning fields', () => {
    const result = validateArtifactCandidate({
      artifactType: 'SalesIntelligenceProfile',
      payload: {
        profiles: [{ company: 'Acme' }],
        profileCount: 1,
      },
    });
    assert.equal(result.ok, false);
    assert.ok(result.errors.some((e) => /reasoning fields/i.test(e)));
  });

  it('accepts SalesIntelligence with messaging strategy', () => {
    const result = validateArtifactCandidate({
      artifactType: 'SalesIntelligenceProfile',
      payload: {
        profiles: [
          {
            company: 'Acme Law',
            messaging_strategy: { angle: 'overflow capacity' },
          },
        ],
        profileCount: 1,
      },
    });
    assert.equal(result.ok, true);
  });

  it('looksLikeNaturalLanguage / isViableCompanyName helpers', () => {
    assert.equal(looksLikeNaturalLanguage('Reuse existing ProspectList'), true);
    assert.equal(looksLikeNaturalLanguage('Build Campaign 001'), true);
    assert.equal(isViableCompanyName('Granite State Law'), true);
    assert.equal(isViableCompanyName('Queen City CPA'), true);
    assert.equal(isViableCompanyName('Reuse existing ProspectList'), false);
  });

  it('exposes platform schema version', () => {
    assert.ok(SCHEMA_VERSION);
    assert.match(String(SCHEMA_VERSION), /^\d+\.\d+/);
  });
});

describe('SPEC-052 chat ingress — NL never becomes ProspectList', () => {
  it('does not detect mission prose as ProspectList', () => {
    const detected = detectOperatorProspectListInMessage(
      [
        'Build Campaign 001 for Anchor Cleaning',
        '',
        'Reuse existing ProspectList',
        'Execute the complete pipeline through Sales Intelligence',
      ].join('\n')
    );
    assert.equal(detected.detected, false);
    assert.equal(detected.autoInject, false);
    assert.equal(detected.rejectedAsNaturalLanguage, true);
    assert.ok(detected.validationFailure);
    assert.equal(detected.validationFailure.status, 'FAILED');
    assert.ok(
      detected.validationFailure.reasons.some((r) =>
        /natural language/i.test(r)
      )
    );
  });

  it('still detects CSV prospect blocks', () => {
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
    assert.equal(detected.autoInject, true);
    assert.equal(detected.prospectCount, 2);
  });
});

describe('SPEC-052 Artifact Bus boundary', () => {
  it('quarantines NL ProspectList and does not expose as consumable', () => {
    const bus = createArtifactBus();
    const art = bus.publishArtifact({
      missionId: 'msn_nl',
      artifactType: 'ProspectList',
      payload: {
        prospects: [
          { companyName: 'Reuse existing ProspectList' },
          { companyName: 'Review Human Test results' },
        ],
        prospectCount: 2,
      },
    });
    assert.equal(art.validationStatus, ARTIFACT_VALIDATION_STATUS.QUARANTINED);
    assert.equal(bus.getLatestArtifact('msn_nl', 'ProspectList'), null);
  });

  it('publishes validated ProspectList as consumable', () => {
    const bus = createArtifactBus();
    const art = bus.publishArtifact({
      missionId: 'msn_ok',
      artifactType: 'ProspectList',
      payload: {
        prospects: [{ companyName: 'Bedford Dental Group' }],
        prospectCount: 1,
      },
    });
    assert.ok(
      [
        ARTIFACT_VALIDATION_STATUS.VALID,
        ARTIFACT_VALIDATION_STATUS.VALID_WITH_WARNINGS,
      ].includes(art.validationStatus)
    );
    assert.ok(bus.getLatestArtifact('msn_ok', 'ProspectList'));
  });
});

describe('SPEC-052 MissionEngine stores reviewable failures', () => {
  it('records artifactValidationFailures without injecting NL', async () => {
    const engine = testEngine();
    const mission = await engine.createFromObjective({
      objective: [
        'Build Campaign 001 for Anchor Cleaning',
        '',
        'Reuse existing ProspectList',
        'Execute the complete pipeline',
      ].join('\n'),
      tenantId: 10,
      clientId: 10,
    });

    assert.ok(
      !mission.operatorProspectList ||
        mission.operatorProspectList.injected !== true
    );
    const failures =
      (mission.deliverables &&
        mission.deliverables.artifactValidationFailures) ||
      [];
    assert.ok(failures.length >= 1);
    assert.equal(failures[0].status, 'FAILED');
    assert.equal(failures[0].artifactType, 'ProspectList');

    const workspace = await engine.getWorkspace(mission.id);
    assert.ok(
      Array.isArray(workspace.artifactValidationFailures) &&
        workspace.artifactValidationFailures.length >= 1
    );

    const bus = createArtifactBus({
      snapshot:
        (mission.deliverables && mission.deliverables.artifactBus) || null,
    });
    const operatorList = bus
      .getArtifactHistory(mission.id, 'ProspectList')
      .find(
        (a) =>
          a.producer === 'operator_import' || a.producer === 'operator_manual'
      );
    assert.equal(operatorList, undefined);
  });
});
