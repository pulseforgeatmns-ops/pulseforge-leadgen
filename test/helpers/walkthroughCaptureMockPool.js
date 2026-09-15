'use strict';

const PROSPECT_ID = '11111111-1111-4111-8111-111111111111';
const ACTION_ID = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa';

function createWalkthroughCaptureMockPool(initial = {}) {
  const state = {
    prospects: [...(initial.prospects || [])],
    agentActions: [],
    lifecycleEvents: [],
    phase3d: initial.phase3d !== false,
    nextProspectId: initial.nextProspectId || PROSPECT_ID,
    nextActionId: initial.nextActionId || ACTION_ID,
    actionIdCounter: 0,
  };

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
        state.actionIdCounter += 1;
        const id = state.actionIdCounter === 1 ? state.nextActionId : `${state.nextActionId}-2`;
        const payload = JSON.parse(params[4]);
        state.agentActions.push({ id, payload, params });
        return { rows: [{ id }] };
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

module.exports = {
  createWalkthroughCaptureMockPool,
  WALKTHROUGH_MOCK_PROSPECT_ID: PROSPECT_ID,
  WALKTHROUGH_MOCK_ACTION_ID: ACTION_ID,
};
