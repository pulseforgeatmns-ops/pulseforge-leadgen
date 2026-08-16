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
];

const INTEREST_LEVELS = ['low', 'medium', 'high'];
const TASK_STATUSES = ['open', 'done', 'rescheduled', 'escalated', 'cancelled'];
const TASK_PRIORITIES = ['normal', 'high', 'warm'];
const ATTRIBUTION_SOURCES = ['ao_field_visit', 'direct_mail_campaign'];
const ESCALATION_STATUSES = ['new', 'seen', 'in_progress', 'resolved'];
const MAX_MODES = ['log_visit', 'follow_up', 'direct_mail_follow_up', 'book_walkthrough', 'daily_debrief', 'ask_for_help'];

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
      interest_level TEXT NOT NULL DEFAULT 'medium'
        CHECK (interest_level IN ('low', 'medium', 'high')),
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
    ALTER TABLE ao_escalations ADD COLUMN IF NOT EXISTS probe_answers JSONB
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_ao_leads_owner ON ao_leads(ao_owner_id, client_id);
    CREATE INDEX IF NOT EXISTS idx_ao_leads_next_follow_up ON ao_leads(next_follow_up_date);
    CREATE INDEX IF NOT EXISTS idx_ao_tasks_owner_due ON ao_follow_up_tasks(ao_owner_id, due_date, status);
    CREATE INDEX IF NOT EXISTS idx_ao_escalations_status ON ao_escalations(status, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_ao_leads_campaign ON ao_leads(client_id, campaign_name)
      WHERE campaign_name IS NOT NULL;
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
  ensureAoFieldSchema,
};
