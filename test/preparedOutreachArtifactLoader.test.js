'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  loadPreparedOutreachCadence,
} = require('../services/preparedOutreachArtifactLoader');
const {
  ensurePreparedCadenceAnnotationSchema,
  persistPreparedCadenceAnnotation,
} = require('../services/preparedCadenceAnnotationPersistence');
const {
  buildHistoricalCadenceAnnotation,
  resolveOutreachSequenceAtPrepare,
  SOURCE_KINDS,
  CADENCE_PROVENANCE,
} = require('../packages/acquisition-mission/PreparedOutreachSequence');

const TEST_CATALOG = {
  anchor_law_firm_draft: [{ day: 0 }, { day: 4 }, { day: 8 }, { day: 13 }],
};
const TEST_CLIENT_MAP = { 10: { law_firm: 'anchor_law_firm_draft' } };

function createMemoryPool() {
  const tables = {
    acquisition_mission_contributions: new Map(),
    acquisition_mission_prepared_cadence_annotations: new Map(),
  };

  const pool = {
    tables,
    async query(sql, params = []) {
      const text = String(sql).replace(/\s+/g, ' ').trim();
      if (/CREATE TABLE|CREATE INDEX/i.test(text)) return { rows: [], rowCount: 0 };
      if (/INSERT INTO acquisition_mission_prepared_cadence_annotations/i.test(text)) {
        const id = params[0];
        tables.acquisition_mission_prepared_cadence_annotations.set(id, {
          id,
          mission_id: params[1],
          tenant_id: params[2],
          execution_record_id: params[3],
          execution_approval_contribution_id: params[4],
          prepared_artifact_revision: params[5],
          prospect_id: params[6],
          outreach_sequence: JSON.parse(params[7]),
          source: JSON.parse(params[8]),
          backfilled_at: params[9],
          created_at: params[10],
        });
        return { rows: [], rowCount: 1 };
      }
      if (/FROM acquisition_mission_prepared_cadence_annotations/i.test(text)) {
        const rows = [...tables.acquisition_mission_prepared_cadence_annotations.values()];
        if (/execution_record_id = \$1/i.test(text)) {
          return { rows: rows.filter((row) => row.execution_record_id === params[0]) };
        }
        return { rows: [] };
      }
      if (/FROM acquisition_mission_contributions/i.test(text)) {
        if (/WHERE id = \$1/i.test(text)) {
          const row = tables.acquisition_mission_contributions.get(params[0]);
          return { rows: row ? [row] : [] };
        }
        if (/WHERE mission_id = \$1/i.test(text)) {
          return {
            rows: [...tables.acquisition_mission_contributions.values()]
              .filter((row) => row.mission_id === params[0]),
          };
        }
      }
      return { rows: [], rowCount: 0 };
    },
  };
  return pool;
}

describe('preparedOutreachArtifactLoader', () => {
  it('prefers approval snapshot cadence over historical annotation', async () => {
    const pool = createMemoryPool();
    await ensurePreparedCadenceAnnotationSchema(pool);

    pool.tables.acquisition_mission_contributions.set('approval-1', {
      id: 'approval-1',
      mission_id: 'mission-1',
      tenant_id: '10',
      specialist: 'operator',
      kind: 'approval',
      payload: {
        decisionKind: 'execution_approval',
        preparedArtifactRevision: 'rev-1',
        paigeContributionId: 'paige-1',
        outreachSequence: {
          steps: [{ step: 0, day: 0 }, { step: 1, day: 4 }],
        },
      },
      at: '2026-09-01T00:00:00.000Z',
    });

    const outreachSequence = resolveOutreachSequenceAtPrepare({
      mission: { tenantId: '10', clientId: 10, targetSegment: 'Law Firms' },
      contributions: [],
      catalog: TEST_CATALOG,
      clientSequenceMap: TEST_CLIENT_MAP,
    });
    const annotation = buildHistoricalCadenceAnnotation({
      missionId: 'mission-1',
      tenantId: '10',
      executionRecordId: 'amo_send_1',
      executionApprovalContributionId: 'approval-1',
      preparedArtifactRevision: 'rev-1',
      outreachSequence,
    });
    await persistPreparedCadenceAnnotation(annotation, pool, { skipEnsure: true });

    const loaded = await loadPreparedOutreachCadence({
      missionId: 'mission-1',
      preparedArtifactRevision: 'rev-1',
      executionApprovalContributionId: 'approval-1',
      executionRecordId: 'amo_send_1',
    }, pool);

    assert.equal(loaded.cadenceProvenance, CADENCE_PROVENANCE.APPROVAL_SNAPSHOT);
    assert.equal(loaded.steps.length, 2);
  });

  it('uses historical annotation when approval lacks cadence', async () => {
    const pool = createMemoryPool();
    await ensurePreparedCadenceAnnotationSchema(pool);

    pool.tables.acquisition_mission_contributions.set('approval-old', {
      id: 'approval-old',
      mission_id: 'mission-1',
      tenant_id: '10',
      specialist: 'operator',
      kind: 'approval',
      payload: {
        decisionKind: 'execution_approval',
        preparedArtifactRevision: 'rev-old',
        paigeContributionId: 'paige-old',
      },
      at: '2026-09-01T00:00:00.000Z',
    });

    const outreachSequence = resolveOutreachSequenceAtPrepare({
      mission: { tenantId: '10', clientId: 10, targetSegment: 'Law Firms' },
      contributions: [],
      catalog: TEST_CATALOG,
      clientSequenceMap: TEST_CLIENT_MAP,
    });
    const annotation = buildHistoricalCadenceAnnotation({
      missionId: 'mission-1',
      tenantId: '10',
      executionRecordId: 'amo_send_backus',
      executionApprovalContributionId: 'approval-old',
      preparedArtifactRevision: 'rev-old',
      outreachSequence,
      templateKey: 'anchor_law_firm_draft',
    });
    await persistPreparedCadenceAnnotation(annotation, pool, { skipEnsure: true });

    const loaded = await loadPreparedOutreachCadence({
      missionId: 'mission-1',
      preparedArtifactRevision: 'rev-old',
      executionApprovalContributionId: 'approval-old',
      executionRecordId: 'amo_send_backus',
    }, pool);

    assert.equal(loaded.cadenceProvenance, CADENCE_PROVENANCE.HISTORICAL_ANNOTATION);
    assert.equal(loaded.reconstructed, true);
    assert.equal(loaded.source.kind, SOURCE_KINDS.HISTORICAL_BACKFILL);
    assert.deepEqual(loaded.steps.map((row) => row.day), [0, 4, 8, 13]);
  });

  it('falls back to Paige contribution cadence', async () => {
    const pool = createMemoryPool();
    const outreachSequence = resolveOutreachSequenceAtPrepare({
      mission: { tenantId: '10', clientId: 10, targetSegment: 'Law Firms' },
      contributions: [],
      catalog: TEST_CATALOG,
      clientSequenceMap: TEST_CLIENT_MAP,
    });

    pool.tables.acquisition_mission_contributions.set('approval-old', {
      id: 'approval-old',
      mission_id: 'mission-1',
      tenant_id: '10',
      specialist: 'operator',
      kind: 'approval',
      payload: {
        decisionKind: 'execution_approval',
        preparedArtifactRevision: 'rev-old',
        paigeContributionId: 'paige-1',
      },
      at: '2026-09-01T00:00:00.000Z',
    });
    pool.tables.acquisition_mission_contributions.set('paige-1', {
      id: 'paige-1',
      mission_id: 'mission-1',
      tenant_id: '10',
      specialist: 'paige',
      kind: 'variants',
      payload: {
        variants: [{ label: 'Primary', subject: 'Hi', body: 'Body' }],
        outreachSequence,
      },
      at: '2026-09-02T00:00:00.000Z',
    });

    const loaded = await loadPreparedOutreachCadence({
      missionId: 'mission-1',
      preparedArtifactRevision: 'rev-old',
      executionApprovalContributionId: 'approval-old',
    }, pool);

    assert.equal(loaded.cadenceProvenance, CADENCE_PROVENANCE.PAIGE_CONTRIBUTION);
    assert.equal(loaded.steps.length, 4);
  });
});
