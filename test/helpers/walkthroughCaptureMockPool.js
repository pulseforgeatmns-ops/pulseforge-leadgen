'use strict';

const PROSPECT_ID = '11111111-1111-4111-8111-111111111111';
const ACTION_ID = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';
const REVIEW_ID = 'cccccccc-cccc-4ccc-8ccc-cccccccccccc';

function createWalkthroughCaptureMockPool(initial = {}) {
  const state = {
    prospects: [...(initial.prospects || [])],
    agentActions: [],
    lifecycleEvents: [],
    phase3d: initial.phase3d !== false,
    nextProspectId: initial.nextProspectId || PROSPECT_ID,
    nextActionId: initial.nextActionId || ACTION_ID,
    nextReviewId: initial.nextReviewId || REVIEW_ID,
    actionIdCounter: 0,
    reviewIdCounter: 0,
  };

  function pushAgentAction(params) {
    state.actionIdCounter += 1;
    const actionType = params[1];
    let id;
    if (actionType === 'lead_qualification_review') {
      state.reviewIdCounter += 1;
      id = state.reviewIdCounter === 1 ? state.nextReviewId : `${state.nextReviewId}-2`;
    } else {
      id = state.actionIdCounter === 1 ? state.nextActionId : `${state.nextActionId}-2`;
    }
    const row = {
      id,
      payload: JSON.parse(params[4]),
      params,
      action_type: actionType,
      status: 'pending',
      client_id: params[5],
      created_by: params[0],
      title: params[2],
      description: params[3],
      executed_at: null,
      created_at: new Date().toISOString(),
      result: null,
    };
    state.agentActions.push(row);
    return { rows: [{ id, payload: row.payload }] };
  }

  const db = {
    state,
    query: async (sql, params = []) => {
      const text = String(sql);

      if (/information_schema\.columns/i.test(text) && /is_synthetic/i.test(text)) {
        return { rows: state.phase3d ? [{ column_name: 'is_synthetic' }] : [] };
      }

      if (/CREATE TABLE IF NOT EXISTS prospect_lifecycle_events/i.test(text)
        || /ALTER TABLE prospect_lifecycle_events/i.test(text)
        || /CREATE (UNIQUE )?INDEX IF NOT EXISTS prospect_lifecycle/i.test(text)
        || /ALTER TABLE clients/i.test(text)
        || /CREATE TABLE IF NOT EXISTS prospect_notes/i.test(text)
        || /CREATE INDEX IF NOT EXISTS prospect_notes/i.test(text)) {
        return { rows: [] };
      }

      if (/ALTER TABLE prospects/i.test(text) && /setter_visibility_reason/i.test(text)) {
        return { rows: [] };
      }

      if (/INSERT INTO prospects/i.test(text)) {
        const email = params[3];
        const clientId = params[11];
        const existing = state.prospects.find((row) => row.email === email);
        if (existing) return { rows: [] };
        const id = state.nextProspectId;
        const row = {
          id,
          client_id: clientId,
          email,
          source: params[9],
          status: 'cold',
          setter_status: 'new',
          icp_score: 80,
          service_area_match: params[12] || 'Manchester',
          do_not_contact: false,
          setter_visible: false,
          setter_visibility_reason: null,
          acquisition_metadata: {},
          acquisition_source: null,
        };
        state.prospects.push(row);
        return { rows: [{ id, client_id: clientId }] };
      }

      if (/SELECT id, client_id FROM prospects WHERE email = \$1/i.test(text)) {
        const email = params[0];
        return {
          rows: state.prospects
            .filter((row) => row.email === email)
            .map((row) => ({ id: row.id, client_id: row.client_id })),
        };
      }

      if (/SELECT id, client_id, setter_status FROM prospects WHERE id = \$1 LIMIT 1/i.test(text)) {
        const row = state.prospects.find((p) => p.id === params[0]);
        return { rows: row ? [{ id: row.id, client_id: row.client_id, setter_status: row.setter_status }] : [] };
      }

      if (/SELECT status, setter_status FROM prospects WHERE id = \$1 AND client_id = \$2/i.test(text)) {
        const row = state.prospects.find((p) => p.id === params[0] && p.client_id === params[1]);
        return { rows: row ? [{ status: row.status, setter_status: row.setter_status }] : [] };
      }

      if (/UPDATE prospects/i.test(text) && /acquisition_metadata/i.test(text)) {
        const row = state.prospects.find((p) => p.id === params[0] && p.client_id === params[2]);
        if (row) {
          const patch = JSON.parse(params[1]);
          row.acquisition_metadata = { ...row.acquisition_metadata, ...patch };
          if (/acquisition_source = COALESCE/i.test(text)) {
            row.acquisition_source = row.acquisition_source || params[3];
          }
        }
        return { rowCount: row ? 1 : 0 };
      }

      if (/UPDATE prospects/i.test(text) && /vertical = \$2/i.test(text)) {
        const row = state.prospects.find((p) => p.id === params[0] && p.client_id === params[3]);
        if (row) {
          row.vertical = params[1];
          row.notes = params[2];
          row.status = 'warm';
        }
        return { rowCount: row ? 1 : 0 };
      }

      if (/SELECT id, client_id, status, do_not_contact, icp_score[\s\S]*FROM prospects[\s\S]*FOR UPDATE/i.test(text)) {
        const row = state.prospects.find((p) => {
          if (p.id !== params[0]) return false;
          if (params[1] != null && Number(p.client_id) !== Number(params[1])) return false;
          if (params[2] != null && p.source !== params[2]) return false;
          return true;
        });
        return { rows: row ? [{ ...row }] : [] };
      }

      if (/UPDATE prospects[\s\S]*setter_visible = \$2/i.test(text)) {
        const row = state.prospects.find((p) => p.id === params[0] && Number(p.client_id) === Number(params[3]));
        if (row) {
          row.setter_visible = params[1];
          row.setter_visibility_reason = params[2];
        }
        return { rows: row ? [{ ...row }] : [] };
      }

      if (/SELECT \* FROM prospects[\s\S]*FOR UPDATE/i.test(text)) {
        const row = state.prospects.find((p) => p.id === params[0] && p.client_id === params[1]);
        return { rows: row ? [{ ...row }] : [] };
      }

      if (/INSERT INTO agent_actions/i.test(text)) {
        return pushAgentAction(params);
      }

      if (/SELECT id, payload, status[\s\S]*FROM agent_actions[\s\S]*action_type = \$2[\s\S]*payload->>'prospect_id'/i.test(text)) {
        const [clientId, actionType, prospectId] = params;
        const row = state.agentActions.find((a) =>
          Number(a.client_id) === Number(clientId)
          && a.action_type === actionType
          && a.status === 'pending'
          && String(a.payload?.prospect_id) === String(prospectId));
        return { rows: row ? [{ id: row.id, payload: row.payload, status: row.status }] : [] };
      }

      if (/SELECT id, client_id, action_type, payload[\s\S]*FROM agent_actions[\s\S]*WHERE id = \$1/i.test(text)) {
        const row = state.agentActions.find((a) => a.id === params[0]);
        return {
          rows: row
            ? [{ id: row.id, client_id: row.client_id, action_type: row.action_type, payload: row.payload }]
            : [],
        };
      }

      if (/SELECT id, payload, status, client_id, action_type[\s\S]*FROM agent_actions[\s\S]*WHERE id = \$1/i.test(text)) {
        const row = state.agentActions.find((a) => a.id === params[0]);
        return {
          rows: row
            ? [{
              id: row.id,
              payload: row.payload,
              status: row.status,
              client_id: row.client_id,
              action_type: row.action_type,
            }]
            : [],
        };
      }

      if (/UPDATE agent_actions[\s\S]*SET payload = \$2::jsonb[\s\S]*status = 'executed'/i.test(text)) {
        const row = state.agentActions.find((a) => a.id === params[0] && Number(a.client_id) === Number(params[4]) && a.status === 'pending');
        if (row) {
          row.payload = JSON.parse(params[1]);
          row.status = 'executed';
          row.executed_at = params[2];
          row.result = params[3];
        }
        return { rowCount: row ? 1 : 0 };
      }

      if (/UPDATE agent_actions[\s\S]*SET payload = \$2::jsonb[\s\S]*WHERE id = \$1 AND client_id = \$3 AND status = 'pending'/i.test(text)) {
        const row = state.agentActions.find((a) => a.id === params[0] && Number(a.client_id) === Number(params[2]) && a.status === 'pending');
        if (row) row.payload = JSON.parse(params[1]);
        return { rowCount: row ? 1 : 0 };
      }

      if (/INSERT INTO prospect_lifecycle_events/i.test(text)) {
        state.lifecycleEvents.push({
          client_id: params[0],
          prospect_id: params[1],
          reason: params[5],
          source: params[9],
          payload: JSON.parse(params[10]),
        });
        return { rows: [{ id: 'evt-1' }] };
      }

      throw new Error(`Unexpected query: ${text.slice(0, 120)}`);
    },
  };

  return db;
}

function walkthroughActions(state) {
  return state.agentActions.filter((a) => a.action_type === 'walkthrough_request');
}

function qualificationReviews(state) {
  return state.agentActions.filter((a) => a.action_type === 'lead_qualification_review');
}

module.exports = {
  createWalkthroughCaptureMockPool,
  walkthroughActions,
  qualificationReviews,
  WALKTHROUGH_MOCK_PROSPECT_ID: PROSPECT_ID,
  WALKTHROUGH_MOCK_ACTION_ID: ACTION_ID,
  WALKTHROUGH_MOCK_REVIEW_ID: REVIEW_ID,
};
