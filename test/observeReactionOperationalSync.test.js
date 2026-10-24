'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { resetLifecycleSchemaCache } = require('../utils/lifecycleSchema');
const {
  OPERATIONAL_TASK_NEXT_ACTION,
  buildOperationalSyncNoteSource,
  buildOperationalSyncMetadata,
  formatOperationalSyncNoteText,
  pickEffectiveHumanOpenReaction,
  resolveMissionBoundCrmProspect,
  syncObserveReactionOperationalFollowUp,
} = require('../services/observeReactionOperationalSync');

const MISSION_ID = 'mission_backus';
const EXECUTION_ID = 'amo_send_backus';
const MISSION_BOUND_KEY = '001c9b7e-5659-4a54-892c-05493a148f9b';
const CRM_PROSPECT_ID = '7adbb294-b94c-45c0-85df-e040f027ece0';
const CRM_COMPANY_ID = '001c9b7e-5659-4a54-892c-05493a148f9b';
const ANNOTATION_ID = 'cadence_ann_backus';
const LEAD_ID = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee';
const OWNER_ID = 42;

function baseTiming() {
  return {
    kind: 'wait_until',
    waitDays: 4,
    dueAt: '2026-09-18T12:25:57.000Z',
    cadenceSource: 'prepared_sequence',
    cadenceProvenance: 'historical_annotation',
    reconstructed: true,
    clockStart: '2026-09-14T12:25:57.000Z',
    businessDays: false,
  };
}

function createMemoryPool(seed = {}) {
  const tables = {
    acquisition_mission_outbound_executions: new Map([
      [EXECUTION_ID, {
        id: EXECUTION_ID,
        mission_id: MISSION_ID,
        prospect_id: MISSION_BOUND_KEY,
        prepared_artifact_revision: 'rev-backus-capacity-1',
        execution_approval_contribution_id: 'approval-backus',
      }],
    ]),
    acquisition_mission_observe_reactions: new Map(),
    acquisition_mission_candidate_observe_state: new Map(),
    acquisition_mission_prepared_cadence_annotations: new Map([
      [ANNOTATION_ID, {
        id: ANNOTATION_ID,
        mission_id: MISSION_ID,
        execution_record_id: EXECUTION_ID,
        outreach_sequence: { steps: [{ day: 0 }, { day: 4 }, { day: 8 }, { day: 13 }] },
      }],
    ]),
    acquisition_mission_contributions: new Map(),
    prospects: new Map([
      [CRM_PROSPECT_ID, {
        id: CRM_PROSPECT_ID,
        prospect_id: CRM_PROSPECT_ID,
        client_id: 10,
        company_id: CRM_COMPANY_ID,
        company_name: 'Backus, Meyer & Branch, LLP',
        icp_score: 88,
        is_synthetic: false,
      }],
    ]),
    companies: new Map([
      [CRM_COMPANY_ID, { id: CRM_COMPANY_ID, name: 'Backus, Meyer & Branch, LLP' }],
    ]),
    ao_leads: new Map([
      [LEAD_ID, {
        id: LEAD_ID,
        client_id: 10,
        business_name: 'Backus, Meyer & Branch, LLP',
        status: 'new_visit',
        interest_level: 'medium',
        ao_owner_id: OWNER_ID,
        crm_prospect_id: null,
      }],
    ]),
    ao_follow_up_tasks: new Map(),
    prospect_notes: new Map(),
  };

  Object.assign(tables, seed.tables || {});

  const pool = {
    tables,
    async query(sql, params = []) {
      const text = String(sql).replace(/\s+/g, ' ').trim();

      if (/CREATE TABLE|CREATE INDEX|ALTER TABLE|ADD CONSTRAINT|ADD COLUMN/i.test(text)) {
        return { rows: [], rowCount: 0 };
      }

      if (/FROM acquisition_mission_outbound_executions/i.test(text)) {
        const row = tables.acquisition_mission_outbound_executions.get(params[0]);
        return { rows: row ? [row] : [] };
      }

      if (/FROM acquisition_mission_observe_reactions/i.test(text)) {
        const rows = [...tables.acquisition_mission_observe_reactions.values()]
          .filter((row) => row.mission_id === params[0]);
        const byObservation = new Map();
        for (const row of rows) {
          const existing = byObservation.get(row.observation_id);
          if (!existing || row.evaluation_sequence > existing.evaluation_sequence) {
            byObservation.set(row.observation_id, row);
          }
        }
        return { rows: [...byObservation.values()] };
      }

      if (/FROM acquisition_mission_candidate_observe_state/i.test(text)) {
        const key = `${params[0]}:${params[1]}`;
        const row = tables.acquisition_mission_candidate_observe_state.get(key);
        return { rows: row ? [row] : [] };
      }

      if (/FROM acquisition_mission_prepared_cadence_annotations/i.test(text)) {
        const rows = [...tables.acquisition_mission_prepared_cadence_annotations.values()]
          .filter((row) => row.execution_record_id === params[0]);
        return { rows };
      }

      if (/FROM prospects p/i.test(text)) {
        if (/company_id::text = \$2/i.test(text)) {
          const missionBoundKey = String(params[1]);
          const matches = [...tables.prospects.values()]
            .filter((row) => (
              row.client_id === params[0]
              && (
                String(row.company_id) === missionBoundKey
                || String(row.id) === missionBoundKey
                || String(row.prospect_id) === missionBoundKey
              )
            ))
            .sort((a, b) => {
              const aCompanyMatch = String(a.company_id) === missionBoundKey ? 0 : 1;
              const bCompanyMatch = String(b.company_id) === missionBoundKey ? 0 : 1;
              if (aCompanyMatch !== bCompanyMatch) return aCompanyMatch - bCompanyMatch;
              return (b.icp_score || 0) - (a.icp_score || 0);
            });
          return { rows: matches.slice(0, 1) };
        }
        const prospect = tables.prospects.get(params[0]);
        if (!prospect) return { rows: [] };
        const company = tables.companies.get(prospect.company_id);
        return { rows: [{ company_name: company?.name || prospect.company_name || company?.name || null }] };
      }

      if (/FROM ao_leads/i.test(text)) {
        if (/crm_prospect_id = \$2/i.test(text)) {
          const row = [...tables.ao_leads.values()]
            .find((lead) => lead.client_id === params[0] && lead.crm_prospect_id === params[1]);
          return { rows: row ? [row] : [] };
        }
        const normalized = String(params[1]).replace(/[^a-z0-9]/gi, '').toLowerCase();
        const row = [...tables.ao_leads.values()].find((lead) => (
          lead.client_id === params[0]
          && String(lead.business_name).replace(/[^a-z0-9]/gi, '').toLowerCase() === normalized
        ));
        return { rows: row ? [row] : [] };
      }

      if (/FROM prospect_notes/i.test(text)) {
        const row = [...tables.prospect_notes.values()].find((note) => (
          note.client_id === params[0]
          && note.prospect_id === params[1]
          && note.source === params[2]
        ));
        return { rows: row ? [row] : [] };
      }

      if (/FROM ao_follow_up_tasks/i.test(text)) {
        const row = [...tables.ao_follow_up_tasks.values()].find((task) => (
          task.lead_id === params[0]
          && task.next_action === params[1]
          && task.status === 'open'
        ));
        return { rows: row ? [row] : [] };
      }

      if (/UPDATE ao_leads/i.test(text)) {
        const lead = tables.ao_leads.get(params[0]);
        lead.status = 'needs_follow_up';
        lead.interest_level = 'high';
        if (!lead.crm_prospect_id) lead.crm_prospect_id = params[1];
        lead.next_follow_up_date = params[2];
        tables.ao_leads.set(lead.id, lead);
        return {
          rows: [{
            id: lead.id,
            status: lead.status,
            interest_level: lead.interest_level,
            crm_prospect_id: lead.crm_prospect_id,
            next_follow_up_date: lead.next_follow_up_date,
          }],
        };
      }

      if (/UPDATE ao_follow_up_tasks/i.test(text)) {
        const task = [...tables.ao_follow_up_tasks.values()].find((row) => row.id === params[0]);
        task.due_date = params[1];
        task.priority = 'warm';
        task.last_interaction_summary = params[2];
        return { rows: [task] };
      }

      if (/INSERT INTO ao_follow_up_tasks/i.test(text)) {
        const id = `task_${tables.ao_follow_up_tasks.size + 1}`;
        const task = {
          id,
          lead_id: params[0],
          ao_owner_id: params[1],
          due_date: params[2],
          status: 'open',
          priority: 'warm',
          next_action: params[3],
          last_interaction_summary: params[4],
          waiting_on_jake: false,
        };
        tables.ao_follow_up_tasks.set(id, task);
        return { rows: [task] };
      }

      if (/INSERT INTO prospect_notes/i.test(text)) {
        const id = `note_${tables.prospect_notes.size + 1}`;
        const note = {
          id,
          client_id: params[0],
          prospect_id: params[1],
          note_type: 'research',
          text: params[2],
          author_name: 'SPEC-252 observe sync',
          source: params[3],
        };
        tables.prospect_notes.set(id, note);
        return { rows: [note] };
      }

      if (text === 'BEGIN' || text === 'COMMIT' || text === 'ROLLBACK') {
        return { rows: [], rowCount: 0 };
      }

      return { rows: [], rowCount: 0 };
    },
    connect() {
      return Promise.resolve({
        query: (...args) => pool.query(...args),
        release() {},
      });
    },
  };

  return pool;
}

function seedReactions(pool) {
  pool.tables.acquisition_mission_observe_reactions.set('obsrx_initial', {
    id: 'obsrx_obs_human_open',
    observation_id: 'obs_human_open',
    mission_id: MISSION_ID,
    evidence_type: 'human_open',
    evidence_strength: 'engagement',
    updated_disposition: 'seen',
    recommended_next_action: 'wait',
    recommended_timing: { kind: 'unresolved', cadenceSource: 'unresolved' },
    evaluation_kind: 'initial',
    evaluation_sequence: 0,
    at: '2026-09-14T12:25:57.000Z',
  });
  pool.tables.acquisition_mission_observe_reactions.set('obsrx_reeval', {
    id: 'obsrx_obs_human_open_reeval_abc',
    observation_id: 'obs_human_open',
    mission_id: MISSION_ID,
    evidence_type: 'human_open',
    evidence_strength: 'engagement',
    updated_disposition: 'seen',
    recommended_next_action: 'wait',
    recommended_timing: baseTiming(),
    evaluation_kind: 'cadence_reevaluation',
    evaluation_sequence: 1,
    reevaluation_trigger_id: ANNOTATION_ID,
    at: '2026-09-14T13:00:00.000Z',
  });
  pool.tables.acquisition_mission_candidate_observe_state.set(`${MISSION_ID}:${MISSION_BOUND_KEY}`, {
    mission_id: MISSION_ID,
    prospect_id: MISSION_BOUND_KEY,
    disposition: 'seen',
    evidence_strength: 'engagement',
    recommended_next_action: 'wait',
    recommended_timing: baseTiming(),
  });
}

describe('observeReactionOperationalSync', () => {
  it('buildOperationalSyncNoteSource is deterministic per annotation', () => {
    resetLifecycleSchemaCache();
    assert.equal(
      buildOperationalSyncNoteSource(ANNOTATION_ID),
      `spec252_observe_reeval:${ANNOTATION_ID}`
    );
  });

  it('buildOperationalSyncMetadata preserves SPEC-252 timing and provenance verbatim', () => {
    const timing = baseTiming();
    const metadata = buildOperationalSyncMetadata({
      missionId: MISSION_ID,
      executionId: EXECUTION_ID,
      missionBoundKey: MISSION_BOUND_KEY,
      crmProspectId: CRM_PROSPECT_ID,
      effectiveReaction: {
        id: 'obsrx_reeval',
        observationId: 'obs_human_open',
        evidenceType: 'human_open',
        evidenceStrength: 'engagement',
        updatedDisposition: 'seen',
        recommendedNextAction: 'wait',
        recommendedTiming: timing,
        evaluationKind: 'cadence_reevaluation',
        reevaluationTriggerId: ANNOTATION_ID,
      },
      candidateState: {
        disposition: 'seen',
        evidenceStrength: 'engagement',
        recommendedNextAction: 'wait',
        recommendedTiming: timing,
      },
      preparedCadence: {
        cadenceSource: 'prepared_sequence',
        cadenceProvenance: 'historical_annotation',
        reconstructed: true,
        steps: [{ day: 0 }, { day: 4 }, { day: 8 }, { day: 13 }],
      },
      annotation: { id: ANNOTATION_ID },
    });

    assert.equal(metadata.recommendedTiming.kind, 'wait_until');
    assert.equal(metadata.recommendedTiming.waitDays, 4);
    assert.equal(metadata.recommendedTiming.dueAt, '2026-09-18T12:25:57.000Z');
    assert.equal(metadata.recommendedTiming.cadenceSource, 'prepared_sequence');
    assert.equal(metadata.recommendedTiming.cadenceProvenance, 'historical_annotation');
    assert.equal(metadata.recommendedTiming.reconstructed, true);
    assert.deepEqual(metadata.sequenceStepDays, [0, 4, 8, 13]);
    assert.equal(metadata.candidate.disposition, 'seen');
    assert.equal(metadata.candidate.evidenceStrength, 'engagement');
    assert.equal(metadata.candidate.recommendedNextAction, 'wait');

    const parsed = JSON.parse(formatOperationalSyncNoteText(metadata));
    assert.equal(parsed.recommendedTiming.dueAt, timing.dueAt);
    assert.deepEqual(parsed.sequenceStepDays, [0, 4, 8, 13]);
  });

  it('pickEffectiveHumanOpenReaction prefers cadence re-evaluation over initial', () => {
    const picked = pickEffectiveHumanOpenReaction([
      {
        observationId: 'obs_human_open',
        evidenceType: 'human_open',
        evaluationSequence: 0,
        recommendedTiming: { kind: 'unresolved' },
        at: '2026-09-14T12:25:57.000Z',
      },
      {
        observationId: 'obs_human_open',
        evidenceType: 'human_open',
        evaluationSequence: 1,
        recommendedTiming: baseTiming(),
        at: '2026-09-14T13:00:00.000Z',
      },
    ]);
    assert.equal(picked.recommendedTiming.kind, 'wait_until');
    assert.equal(picked.recommendedTiming.waitDays, 4);
  });

  it('resolveMissionBoundCrmProspect maps company/candidate key to canonical CRM contact', async () => {
    resetLifecycleSchemaCache();
    const pool = createMemoryPool();
    const resolved = await resolveMissionBoundCrmProspect(pool, {
      clientId: 10,
      missionBoundKey: MISSION_BOUND_KEY,
    });
    assert.equal(resolved.crmProspectId, CRM_PROSPECT_ID);
    assert.equal(resolved.crmCompanyId, CRM_COMPANY_ID);
    assert.notEqual(resolved.crmProspectId, MISSION_BOUND_KEY);
  });

  it('sync creates AO follow-up, links canonical CRM prospect, and writes research note', async () => {
    resetLifecycleSchemaCache();
    const pool = createMemoryPool();
    seedReactions(pool);

    const report = await syncObserveReactionOperationalFollowUp(pool, {
      missionId: MISSION_ID,
      executionId: EXECUTION_ID,
      clientId: 10,
      businessName: 'Backus, Meyer & Branch, LLP',
    });

    assert.equal(report.missionBoundKey, MISSION_BOUND_KEY);
    assert.equal(report.crmProspectId, CRM_PROSPECT_ID);
    assert.notEqual(report.missionBoundKey, report.crmProspectId);
    assert.equal(report.actions.leadUpdated, true);
    assert.equal(report.aoLead.crm_prospect_id, CRM_PROSPECT_ID);
    assert.equal(report.actions.crmProspectLinked, true);
    assert.equal(report.actions.taskCreated, true);
    assert.equal(report.actions.noteCreated, true);
    assert.equal(report.aoLead.status, 'needs_follow_up');
    assert.equal(report.aoLead.interest_level, 'high');
    assert.equal(report.task.priority, 'warm');
    assert.equal(report.task.next_action, OPERATIONAL_TASK_NEXT_ACTION);
    assert.equal(report.task.due_date, '2026-09-18');
    assert.equal(report.note.note_type, 'research');
    assert.equal(report.note.prospect_id, CRM_PROSPECT_ID);
    assert.equal(report.note.source, buildOperationalSyncNoteSource(ANNOTATION_ID));

    const notePayload = JSON.parse(report.note.text);
    assert.equal(notePayload.missionBoundKey, MISSION_BOUND_KEY);
    assert.equal(notePayload.crmProspectId, CRM_PROSPECT_ID);
    assert.equal(notePayload.prospectId, MISSION_BOUND_KEY);
    assert.equal(notePayload.recommendedTiming.waitDays, 4);
    assert.equal(notePayload.recommendedTiming.kind, 'wait_until');
  });

  it('sync is idempotent on repeat — updates task, skips duplicate note', async () => {
    resetLifecycleSchemaCache();
    const pool = createMemoryPool();
    seedReactions(pool);

    const first = await syncObserveReactionOperationalFollowUp(pool, {
      missionId: MISSION_ID,
      executionId: EXECUTION_ID,
      clientId: 10,
      businessName: 'Backus, Meyer & Branch, LLP',
    });
    assert.equal(first.actions.noteCreated, true);
    assert.equal(first.actions.taskCreated, true);

    const lead = pool.tables.ao_leads.get(LEAD_ID);
    lead.crm_prospect_id = CRM_PROSPECT_ID;

    const second = await syncObserveReactionOperationalFollowUp(pool, {
      missionId: MISSION_ID,
      executionId: EXECUTION_ID,
      clientId: 10,
      businessName: 'Backus, Meyer & Branch, LLP',
    });

    assert.equal(second.actions.noteSkipped, true);
    assert.equal(second.actions.noteCreated, false);
    assert.equal(second.actions.taskUpdated, true);
    assert.equal(second.actions.taskCreated, false);
    assert.equal(second.actions.crmProspectLinked, false);
    assert.equal(pool.tables.prospect_notes.size, 1);
    assert.equal(pool.tables.ao_follow_up_tasks.size, 1);
  });

  it('rolls back all writes when ao_leads CRM FK would fail', async () => {
    resetLifecycleSchemaCache();
    const pool = createMemoryPool();
    seedReactions(pool);

    const originalConnect = pool.connect.bind(pool);
    pool.connect = async () => {
      const client = await originalConnect();
      const originalQuery = client.query.bind(client);
      client.query = async (sql, params = []) => {
        const text = String(sql).replace(/\s+/g, ' ').trim();
        if (/UPDATE ao_leads/i.test(text) && params[1] === CRM_PROSPECT_ID) {
          const err = new Error('insert or update on table "ao_leads" violates foreign key constraint "ao_leads_crm_prospect_id_fkey"');
          err.code = '23503';
          throw err;
        }
        return originalQuery(sql, params);
      };
      return client;
    };

    await assert.rejects(
      () => syncObserveReactionOperationalFollowUp(pool, {
        missionId: MISSION_ID,
        executionId: EXECUTION_ID,
        clientId: 10,
        businessName: 'Backus, Meyer & Branch, LLP',
      }),
      (err) => err.code === '23503'
    );

    const lead = pool.tables.ao_leads.get(LEAD_ID);
    assert.equal(lead.status, 'new_visit');
    assert.equal(lead.crm_prospect_id, null);
    assert.equal(pool.tables.ao_follow_up_tasks.size, 0);
    assert.equal(pool.tables.prospect_notes.size, 0);
  });
});
