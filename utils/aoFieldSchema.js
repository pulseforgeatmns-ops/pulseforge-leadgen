const pool = require('../db');
const { ensureUsersTable } = require('../middleware/auth');
const { ensureClientArchitecture } = require('./clientContext');

let schemaInitPromise;

const LEAD_STATUSES = [
  'new_visit',
  'decision_maker_absent',
  'needs_follow_up',
  'walkthrough_requested',
  'walkthrough_booked',
  'walkthrough_completed',
  'proposal_needed',
  'closed_won',
  'closed_lost',
  'not_a_fit',
  'do_not_contact',
  'converted_to_crm',
];

const INTEREST_LEVELS = ['low', 'medium', 'high'];
const TASK_STATUSES = ['open', 'done', 'rescheduled', 'escalated', 'cancelled'];
const TASK_PRIORITIES = ['normal', 'high', 'warm'];
const ATTRIBUTION_SOURCES = ['ao_field_visit', 'direct_mail_campaign'];
const ESCALATION_STATUSES = ['new', 'seen', 'in_progress', 'resolved', 'ignored'];
const MAX_MODES = ['log_visit', 'follow_up', 'direct_mail_follow_up', 'route_follow_up', 'phone_follow_up', 'book_walkthrough', 'daily_debrief', 'ask_for_help', 'conversation'];
const REPORT_STATUSES = ['new', 'reviewed', 'resolved'];
const CONVERSATION_STATUSES = ['active', 'done', 'archived', 'closed', 'reopened'];
const ROUTING_ISSUE_TYPES = [
  'wrong_route',
  'wrong_prospect',
  'wrong_mission',
  'lost_context',
  'should_have_opened_brief',
  'should_have_opened_conversation',
  'treated_as_done_incorrectly',
  'other',
];
const ROUTE_SORT_MODES = ['farthest_first', 'closest_first', 'shortest_route', 'manual'];
const ROUTE_START_POINT_TYPES = ['current_location', 'anchor_office', 'custom'];
const ROUTE_STATUSES = ['active', 'completed', 'cancelled'];
const ROUTE_STOP_STATUSES = ['pending', 'done', 'skipped', 'moved_later'];

async function ensureAoFieldSchemaOnce() {
  await ensureClientArchitecture();
  await ensureUsersTable();

  await pool.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS phone TEXT,
    ADD COLUMN IF NOT EXISTS territory TEXT,
    ADD COLUMN IF NOT EXISTS manager_id INTEGER REFERENCES users(id),
    ADD COLUMN IF NOT EXISTS daily_goal INTEGER,
    ADD COLUMN IF NOT EXISTS weekly_goal INTEGER
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ao_leads (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      client_id INTEGER NOT NULL REFERENCES clients(id),
      business_name TEXT NOT NULL,
      address TEXT,
      business_type TEXT,
      status TEXT NOT NULL DEFAULT 'new_visit'
        CHECK (status IN (${LEAD_STATUSES.map(s => `'${s}'`).join(', ')})),
      interest_level TEXT DEFAULT NULL
        CHECK (interest_level IS NULL OR interest_level IN ('low', 'medium', 'high')),
      ao_owner_id INTEGER NOT NULL REFERENCES users(id),
      first_contact_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      last_contact_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      next_follow_up_date DATE,
      next_follow_up_owner_id INTEGER REFERENCES users(id),
      attribution_source TEXT NOT NULL DEFAULT 'ao_field_visit',
      attribution_window_days INTEGER NOT NULL DEFAULT 180,
      commission_eligible BOOLEAN NOT NULL DEFAULT true,
      original_visit_note TEXT,
      probe_answers JSONB,
      closed_revenue_id TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ao_contacts (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      lead_id UUID NOT NULL REFERENCES ao_leads(id) ON DELETE CASCADE,
      contact_name TEXT NOT NULL,
      contact_title TEXT,
      phone TEXT,
      email TEXT,
      is_decision_maker BOOLEAN NOT NULL DEFAULT false,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ao_follow_up_tasks (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      lead_id UUID NOT NULL REFERENCES ao_leads(id) ON DELETE CASCADE,
      contact_id UUID REFERENCES ao_contacts(id) ON DELETE SET NULL,
      ao_owner_id INTEGER NOT NULL REFERENCES users(id),
      due_date DATE NOT NULL,
      status TEXT NOT NULL DEFAULT 'open'
        CHECK (status IN (${TASK_STATUSES.map(s => `'${s}'`).join(', ')})),
      priority TEXT NOT NULL DEFAULT 'normal'
        CHECK (priority IN ('normal', 'high')),
      next_action TEXT,
      last_interaction_summary TEXT,
      suggested_message TEXT,
      waiting_on_jake BOOLEAN NOT NULL DEFAULT false,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      completed_at TIMESTAMPTZ
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ao_escalations (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      lead_id UUID NOT NULL REFERENCES ao_leads(id) ON DELETE CASCADE,
      contact_id UUID REFERENCES ao_contacts(id) ON DELETE SET NULL,
      ao_owner_id INTEGER NOT NULL REFERENCES users(id),
      reason TEXT NOT NULL,
      summary TEXT NOT NULL,
      probe_answers JSONB,
      status TEXT NOT NULL DEFAULT 'new'
        CHECK (status IN (${ESCALATION_STATUSES.map(s => `'${s}'`).join(', ')})),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      resolved_at TIMESTAMPTZ
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ao_max_sessions (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      ao_owner_id INTEGER NOT NULL REFERENCES users(id),
      client_id INTEGER NOT NULL REFERENCES clients(id),
      mode TEXT NOT NULL
        CHECK (mode IN (${MAX_MODES.map(m => `'${m}'`).join(', ')})),
      step_index INTEGER NOT NULL DEFAULT 0,
      payload JSONB NOT NULL DEFAULT '{}'::jsonb,
      mission_id TEXT,
      tenant_id TEXT,
      canonical_ingestion_status TEXT DEFAULT 'pending',
      completed BOOLEAN NOT NULL DEFAULT false,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    ALTER TABLE ao_leads ADD COLUMN IF NOT EXISTS probe_answers JSONB
  `);
  await pool.query(`
    ALTER TABLE ao_leads ADD COLUMN IF NOT EXISTS campaign_name TEXT
  `);
  // prospects.id is UUID — INTEGER FK fails at deploy with "cannot be implemented".
  await pool.query(`
    ALTER TABLE ao_leads ADD COLUMN IF NOT EXISTS crm_prospect_id UUID
  `);
  await pool.query(`
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'ao_leads'
          AND column_name = 'crm_prospect_id'
          AND udt_name = 'int4'
      ) THEN
        ALTER TABLE ao_leads DROP COLUMN crm_prospect_id;
        ALTER TABLE ao_leads ADD COLUMN crm_prospect_id UUID;
      END IF;
    END $$;
  `);
  await pool.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'ao_leads_crm_prospect_id_fkey'
          AND conrelid = 'ao_leads'::regclass
      ) THEN
        ALTER TABLE ao_leads
          ADD CONSTRAINT ao_leads_crm_prospect_id_fkey
          FOREIGN KEY (crm_prospect_id) REFERENCES prospects(id);
      END IF;
    END $$;
  `);

  await pool.query(`
    ALTER TABLE ao_leads DROP CONSTRAINT IF EXISTS ao_leads_status_check
  `);
  await pool.query(`
    ALTER TABLE ao_leads ADD CONSTRAINT ao_leads_status_check
      CHECK (status IN (${LEAD_STATUSES.map(s => `'${s}'`).join(', ')}))
  `);

  await pool.query(`ALTER TABLE ao_leads ALTER COLUMN interest_level DROP NOT NULL`);
  await pool.query(`ALTER TABLE ao_leads ALTER COLUMN interest_level DROP DEFAULT`);
  await pool.query(`
    ALTER TABLE ao_leads DROP CONSTRAINT IF EXISTS ao_leads_interest_level_check
  `);
  await pool.query(`
    ALTER TABLE ao_leads ADD CONSTRAINT ao_leads_interest_level_check
      CHECK (interest_level IS NULL OR interest_level IN ('low', 'medium', 'high'))
  `);

  await pool.query(`
    ALTER TABLE ao_escalations DROP CONSTRAINT IF EXISTS ao_escalations_status_check
  `);
  await pool.query(`
    ALTER TABLE ao_escalations ADD CONSTRAINT ao_escalations_status_check
      CHECK (status IN (${ESCALATION_STATUSES.map(s => `'${s}'`).join(', ')}))
  `);

  await pool.query(`
    ALTER TABLE ao_follow_up_tasks DROP CONSTRAINT IF EXISTS ao_follow_up_tasks_priority_check
  `);
  await pool.query(`
    ALTER TABLE ao_follow_up_tasks ADD CONSTRAINT ao_follow_up_tasks_priority_check
      CHECK (priority IN ('normal', 'high', 'warm'))
  `);

  await pool.query(`
    ALTER TABLE ao_max_sessions DROP CONSTRAINT IF EXISTS ao_max_sessions_mode_check
  `);
  await pool.query(`
    ALTER TABLE ao_max_sessions ADD CONSTRAINT ao_max_sessions_mode_check
      CHECK (mode IN (${MAX_MODES.map(m => `'${m}'`).join(', ')}))
  `);
  await pool.query(`
    ALTER TABLE ao_max_sessions ADD COLUMN IF NOT EXISTS mission_id TEXT
  `);
  await pool.query(`
    ALTER TABLE ao_max_sessions ADD COLUMN IF NOT EXISTS tenant_id TEXT
  `);
  await pool.query(`
    ALTER TABLE ao_max_sessions ADD COLUMN IF NOT EXISTS canonical_ingestion_status TEXT DEFAULT 'pending'
  `);
  await pool.query(`
    ALTER TABLE ao_escalations ADD COLUMN IF NOT EXISTS probe_answers JSONB
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ao_routes (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      client_id INTEGER NOT NULL REFERENCES clients(id),
      ao_owner_id INTEGER NOT NULL REFERENCES users(id),
      queue_filter TEXT NOT NULL DEFAULT 'today',
      sort_mode TEXT NOT NULL DEFAULT 'closest_first'
        CHECK (sort_mode IN (${ROUTE_SORT_MODES.map(s => `'${s}'`).join(', ')})),
      start_point_type TEXT NOT NULL DEFAULT 'current_location'
        CHECK (start_point_type IN (${ROUTE_START_POINT_TYPES.map(s => `'${s}'`).join(', ')})),
      start_lat NUMERIC,
      start_lng NUMERIC,
      start_address TEXT,
      status TEXT NOT NULL DEFAULT 'active'
        CHECK (status IN (${ROUTE_STATUSES.map(s => `'${s}'`).join(', ')})),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      completed_at TIMESTAMPTZ
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ao_route_stops (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      route_id UUID NOT NULL REFERENCES ao_routes(id) ON DELETE CASCADE,
      task_id UUID NOT NULL REFERENCES ao_follow_up_tasks(id),
      lead_id UUID NOT NULL REFERENCES ao_leads(id),
      sequence INTEGER NOT NULL,
      address TEXT,
      lat NUMERIC,
      lng NUMERIC,
      status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN (${ROUTE_STOP_STATUSES.map(s => `'${s}'`).join(', ')})),
      completed_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ao_max_conversation_reports (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      client_id INTEGER NOT NULL REFERENCES clients(id),
      ao_owner_id INTEGER NOT NULL REFERENCES users(id),
      session_id UUID NOT NULL REFERENCES ao_max_sessions(id),
      category TEXT NOT NULL DEFAULT 'user_report',
      note TEXT,
      transcript_json JSONB NOT NULL DEFAULT '[]'::jsonb,
      context_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      status TEXT NOT NULL DEFAULT 'new'
        CHECK (status IN (${REPORT_STATUSES.map(s => `'${s}'`).join(', ')})),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_ao_max_reports_client_created
      ON ao_max_conversation_reports(client_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_ao_max_reports_session
      ON ao_max_conversation_reports(session_id);
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ao_routing_issue_flags (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      ao_user_id TEXT,
      session_id TEXT,
      conversation_id TEXT,
      message_id TEXT,
      prospect_id TEXT,
      mission_id TEXT,
      route_observed JSONB,
      route_expected TEXT,
      issue_type TEXT NOT NULL
        CHECK (issue_type IN (${ROUTING_ISSUE_TYPES.map(t => `'${t}'`).join(', ')})),
      notes TEXT,
      decision_id TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    ALTER TABLE ao_max_sessions
      ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active',
      ADD COLUMN IF NOT EXISTS closed_at TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS reopened_at TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS closed_by TEXT,
      ADD COLUMN IF NOT EXISTS reopened_by TEXT,
      ADD COLUMN IF NOT EXISTS prospect_id TEXT,
      ADD COLUMN IF NOT EXISTS mission_id TEXT
  `);

  await pool.query(`
    UPDATE ao_max_sessions
    SET status = CASE WHEN completed = true THEN 'done' ELSE 'active' END
    WHERE status = 'active' AND completed = true
  `);

  await pool.query(`
    ALTER TABLE ao_max_sessions DROP CONSTRAINT IF EXISTS ao_max_sessions_status_check
  `);
  await pool.query(`
    ALTER TABLE ao_max_sessions ADD CONSTRAINT ao_max_sessions_status_check
      CHECK (status IN (${CONVERSATION_STATUSES.map(s => `'${s}'`).join(', ')}))
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_ao_routing_flags_tenant_created
      ON ao_routing_issue_flags (tenant_id, created_at DESC)
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_ao_max_sessions_owner_status
      ON ao_max_sessions (ao_owner_id, client_id, status, updated_at DESC)
      WHERE mode = 'conversation'
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ao_prospect_updates (
      id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      ao_user_id TEXT,
      prospect_id TEXT NOT NULL,
      outcome_type TEXT NOT NULL
        CHECK (outcome_type IN (
          'called_no_answer', 'left_voicemail', 'sent_email', 'spoke_gatekeeper',
          'spoke_decision_maker', 'booked_assessment', 'not_interested', 'not_fit',
          'follow_up_later', 'needs_research', 'other'
        )),
      notes TEXT,
      next_action TEXT,
      next_action_due_at TIMESTAMPTZ,
      advisory_stage TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_ao_prospect_updates_tenant_ao_created
      ON ao_prospect_updates (tenant_id, ao_user_id, created_at DESC)
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_ao_prospect_updates_prospect_created
      ON ao_prospect_updates (prospect_id, created_at DESC)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_ao_leads_owner ON ao_leads(ao_owner_id, client_id);
    CREATE INDEX IF NOT EXISTS idx_ao_leads_next_follow_up ON ao_leads(next_follow_up_date);
    CREATE INDEX IF NOT EXISTS idx_ao_tasks_owner_due ON ao_follow_up_tasks(ao_owner_id, due_date, status);
    CREATE INDEX IF NOT EXISTS idx_ao_escalations_status ON ao_escalations(status, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_ao_leads_campaign ON ao_leads(client_id, campaign_name)
      WHERE campaign_name IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_ao_routes_owner_active
      ON ao_routes(ao_owner_id, client_id, status)
      WHERE status = 'active';
    CREATE INDEX IF NOT EXISTS idx_ao_route_stops_route_seq
      ON ao_route_stops(route_id, sequence);
  `);
}

async function ensureAoFieldSchema() {
  if (!schemaInitPromise) {
    schemaInitPromise = ensureAoFieldSchemaOnce().catch(err => {
      schemaInitPromise = null;
      throw err;
    });
  }
  return schemaInitPromise;
}

module.exports = {
  LEAD_STATUSES,
  INTEREST_LEVELS,
  TASK_STATUSES,
  TASK_PRIORITIES,
  ATTRIBUTION_SOURCES,
  ESCALATION_STATUSES,
  MAX_MODES,
  REPORT_STATUSES,
  ROUTE_SORT_MODES,
  ROUTE_START_POINT_TYPES,
  ROUTE_STATUSES,
  ROUTE_STOP_STATUSES,
  CONVERSATION_STATUSES,
  ROUTING_ISSUE_TYPES,
  ensureAoFieldSchema,
};
