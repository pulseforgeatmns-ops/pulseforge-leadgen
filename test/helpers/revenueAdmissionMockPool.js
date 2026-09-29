'use strict';

const { randomUUID } = require('crypto');
const {
  createWalkthroughCaptureMockPool,
  qualificationReviews,
  walkthroughActions,
  WALKTHROUGH_MOCK_PROSPECT_ID,
  WALKTHROUGH_MOCK_ACTION_ID,
  WALKTHROUGH_MOCK_REVIEW_ID,
} = require('./walkthroughCaptureMockPool');

function createRevenueAdmissionMockPool(initial = {}) {
  const base = createWalkthroughCaptureMockPool(initial);
  const state = {
    ...base.state,
    opportunities: [...(initial.opportunities || [])],
    revenueEvents: [...(initial.revenueEvents || [])],
    revenueAudit: [],
    revenueMetrics: {},
    featureFlags: initial.featureFlags || {
      client_id: initial.clientId || 10,
      revenue_schema_enabled: true,
      revenue_operator_reads_enabled: true,
      revenue_operator_writes_enabled: true,
      revenue_max_reads_enabled: false,
      revenue_followup_recommendations_enabled: false,
    },
    inTransaction: false,
  };

  function nextOpportunityId() {
    return randomUUID();
  }

  async function handleQuery(text, params = []) {
    const sql = String(text);

    if (/^BEGIN/i.test(sql)) {
      state.inTransaction = true;
      return { rows: [] };
    }
    if (/^COMMIT/i.test(sql)) {
      state.inTransaction = false;
      return { rows: [] };
    }
    if (/^ROLLBACK/i.test(sql)) {
      state.inTransaction = false;
      return { rows: [] };
    }
    if (/pg_advisory_xact_lock/i.test(sql)) {
      return { rows: [{ locked: true }] };
    }

    if (/FROM revenue_feature_flags WHERE client_id = \$1/i.test(sql)) {
      return { rows: [state.featureFlags] };
    }

    if (/SELECT \* FROM opportunities WHERE client_id = \$1 AND id = \$2/i.test(sql)) {
      const row = state.opportunities.find((o) => o.client_id === params[0] && o.id === params[1]);
      return { rows: row ? [{ ...row }] : [] };
    }

    if (/stage NOT IN \('won', 'lost', 'cancelled'\)/i.test(sql)) {
      const row = state.opportunities.find((o) =>
        o.client_id === params[0]
        && o.prospect_id === params[1]
        && !['won', 'lost', 'cancelled'].includes(o.stage));
      return { rows: row ? [{ ...row }] : [] };
    }

    if (/stage IN \('won', 'lost', 'cancelled'\)/i.test(sql)) {
      const matches = state.opportunities
        .filter((o) => o.client_id === params[0] && o.prospect_id === params[1]
          && ['won', 'lost', 'cancelled'].includes(o.stage))
        .sort((a, b) => new Date(b.closed_at || b.updated_at || b.created_at)
          - new Date(a.closed_at || a.updated_at || a.created_at));
      return { rows: matches[0] ? [{ ...matches[0] }] : [] };
    }

    if (/payload_json->'result' AS result[\s\S]*FROM revenue_events/i.test(sql)) {
      const row = state.revenueEvents.find((e) =>
        e.client_id === params[0] && e.source_system === params[1] && e.idempotency_key === params[2]);
      return { rows: row ? [{ result: row.payload_json?.result }] : [] };
    }

    if (/INSERT INTO revenue_operational_metrics/i.test(sql)) {
      const key = params[0];
      state.revenueMetrics[key] = (state.revenueMetrics[key] || 0) + 1;
      return { rows: [] };
    }

    if (/SELECT id, client_id FROM prospects WHERE id = \$1 LIMIT 1/i.test(sql)) {
      const row = state.prospects.find((p) => p.id === params[0]);
      return { rows: row ? [{ id: row.id, client_id: row.client_id }] : [] };
    }

    if (/SELECT \* FROM prospects WHERE client_id = \$1 AND id = \$2/i.test(sql)) {
      const row = state.prospects.find((p) => p.client_id === params[0] && p.id === params[1]);
      if (!row) throw Object.assign(new Error('Record not found'), { code: 'NOT_FOUND', status: 404 });
      return { rows: [{ ...row }] };
    }

    if (/INSERT INTO opportunities/i.test(sql)) {
      const hasMetadata = /attribution_metadata/i.test(sql);
      const id = nextOpportunityId();
      const now = new Date().toISOString();
      const row = {
        id,
        client_id: params[0],
        customer_id: params[1],
        prospect_id: params[2],
        company_id: params[3],
        service_type: params[4],
        estimated_value_cents: params[5],
        estimated_cost_cents: params[6],
        expected_close_date: params[7],
        stage: 'identified',
        source: params[8],
        lead_source_detail: params[9],
        campaign_id: params[10],
        sequence_id: params[11],
        attribution_status: params[12],
        human_owner: params[13],
        attribution_metadata: hasMetadata ? JSON.parse(params[14]) : null,
        created_at: now,
        updated_at: now,
        closed_at: null,
      };
      state.opportunities.push(row);
      return { rows: [{ ...row }] };
    }

    if (/INSERT INTO revenue_events/i.test(sql)) {
      const payload = JSON.parse(params[12]);
      const row = {
        event_id: randomUUID(),
        client_id: params[0],
        event_type: params[1],
        entity_type: params[2],
        entity_id: params[3],
        source_system: params[4],
        occurred_at: params[6],
        actor_type: params[7],
        actor_id: params[8],
        correlation_id: params[9],
        idempotency_key: params[11],
        payload_json: payload,
      };
      state.revenueEvents.push(row);
      return { rows: [{ ...row }] };
    }

    if (/INSERT INTO revenue_operator_audit/i.test(sql)) {
      state.revenueAudit.push({ client_id: params[0], idempotency_key: params[9] });
      return { rows: [] };
    }

    if (/SELECT event_id,event_type[\s\S]*FROM revenue_events WHERE client_id=\$1 AND correlation_id=\$2/i.test(sql)) {
      const rows = state.revenueEvents.filter((e) => e.client_id === params[0] && e.correlation_id === params[1]);
      return { rows };
    }

    if (/UPDATE agent_actions[\s\S]*SET payload = \$2::jsonb[\s\S]*status = 'executed'/i.test(sql)) {
      return base.query(text, params);
    }

    if (/UPDATE agent_actions[\s\S]*SET payload = \$2::jsonb[\s\S]*WHERE id = \$1 AND client_id = \$3 AND status = 'executed'/i.test(sql)) {
      const row = state.agentActions.find((a) => a.id === params[0] && Number(a.client_id) === Number(params[2]) && a.status === 'executed');
      if (row) row.payload = JSON.parse(params[1]);
      return { rowCount: row ? 1 : 0 };
    }

    if (/SELECT id, payload, status, client_id, action_type, executed_at, created_at[\s\S]*FROM agent_actions[\s\S]*WHERE id = \$1/i.test(sql)) {
      const row = state.agentActions.find((a) => a.id === params[0]);
      return {
        rows: row
          ? [{
            id: row.id,
            payload: row.payload,
            status: row.status,
            client_id: row.client_id,
            action_type: row.action_type,
            executed_at: row.executed_at || null,
            created_at: row.created_at || new Date().toISOString(),
          }]
          : [],
      };
    }

    return base.query(text, params);
  }

  const db = {
    state,
    query: handleQuery,
    connect: async () => ({
      query: handleQuery,
      release: () => {},
    }),
  };

  return db;
}

module.exports = {
  createRevenueAdmissionMockPool,
  qualificationReviews,
  walkthroughActions,
  WALKTHROUGH_MOCK_PROSPECT_ID,
  WALKTHROUGH_MOCK_ACTION_ID,
  WALKTHROUGH_MOCK_REVIEW_ID,
};
