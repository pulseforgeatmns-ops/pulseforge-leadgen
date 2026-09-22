'use strict';

const pool = require('../db');
const { ensureClientArchitecture } = require('./clientContext');
const { ensureUsersTable } = require('../middleware/auth');
const {
  PROSPECT_MOTIONS,
  ASSIGNMENT_CATEGORIES,
  DEBRIEF_NEXT_ACTIONS,
  ADVISORY_STAGES,
  DEBRIEF_QUALITIES,
} = require('./aoProspectRoutingConstants');

let schemaInitPromise;

async function ensureAoProspectRoutingSchemaOnce() {
  await ensureClientArchitecture();
  await ensureUsersTable();

  await pool.query(`
    ALTER TABLE prospects
    ADD COLUMN IF NOT EXISTS prospect_motion TEXT,
    ADD COLUMN IF NOT EXISTS ao_fit_score INTEGER,
    ADD COLUMN IF NOT EXISTS ao_fit_reason TEXT,
    ADD COLUMN IF NOT EXISTS assigned_ao_id INTEGER REFERENCES users(id),
    ADD COLUMN IF NOT EXISTS ao_assignment_reason TEXT,
    ADD COLUMN IF NOT EXISTS ao_assignment_category TEXT,
    ADD COLUMN IF NOT EXISTS recommended_angle TEXT,
    ADD COLUMN IF NOT EXISTS recommended_first_action TEXT,
    ADD COLUMN IF NOT EXISTS advisory_stage TEXT DEFAULT 'unassigned',
    ADD COLUMN IF NOT EXISTS last_debrief_status TEXT,
    ADD COLUMN IF NOT EXISTS next_action TEXT,
    ADD COLUMN IF NOT EXISTS next_action_owner TEXT,
    ADD COLUMN IF NOT EXISTS next_action_due_at TIMESTAMPTZ
  `);

  await pool.query(`ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_prospect_motion_check`);
  await pool.query(`
    ALTER TABLE prospects ADD CONSTRAINT prospects_prospect_motion_check
      CHECK (prospect_motion IS NULL OR prospect_motion IN (${PROSPECT_MOTIONS.map(v => `'${v}'`).join(', ')}))
  `);
  await pool.query(`ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_ao_assignment_category_check`);
  await pool.query(`
    ALTER TABLE prospects ADD CONSTRAINT prospects_ao_assignment_category_check
      CHECK (ao_assignment_category IS NULL OR ao_assignment_category IN (${ASSIGNMENT_CATEGORIES.map(v => `'${v}'`).join(', ')}))
  `);
  await pool.query(`ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_advisory_stage_check`);
  await pool.query(`
    ALTER TABLE prospects ADD CONSTRAINT prospects_advisory_stage_check
      CHECK (advisory_stage IS NULL OR advisory_stage IN (${ADVISORY_STAGES.map(v => `'${v}'`).join(', ')}))
  `);
  await pool.query(`ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_next_action_check`);
  await pool.query(`
    ALTER TABLE prospects ADD CONSTRAINT prospects_next_action_check
      CHECK (next_action IS NULL OR next_action IN (${DEBRIEF_NEXT_ACTIONS.map(v => `'${v}'`).join(', ')}))
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ao_prospect_tasks (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      client_id INTEGER NOT NULL REFERENCES clients(id),
      prospect_id UUID NOT NULL REFERENCES prospects(id),
      assigned_ao_id INTEGER NOT NULL REFERENCES users(id),
      assignment_category TEXT NOT NULL
        CHECK (assignment_category IN (${ASSIGNMENT_CATEGORIES.map(v => `'${v}'`).join(', ')})),
      motion TEXT NOT NULL
        CHECK (motion IN ('EMAIL_LED', 'AO_LED', 'HYBRID', 'NURTURE')),
      priority TEXT NOT NULL DEFAULT 'normal'
        CHECK (priority IN ('normal', 'high', 'warm')),
      account_name TEXT,
      segment TEXT,
      location TEXT,
      why_account_matters TEXT,
      recommended_angle TEXT,
      first_action TEXT,
      discovery_objective TEXT,
      suggested_opener TEXT,
      desired_next_outcome TEXT,
      required_log_fields JSONB NOT NULL DEFAULT '[]'::jsonb,
      deadline DATE,
      required_debrief BOOLEAN NOT NULL DEFAULT true,
      status TEXT NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'in_progress', 'completed', 'cancelled')),
      routing_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      completed_at TIMESTAMPTZ
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ao_advisory_debriefs (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      client_id INTEGER NOT NULL REFERENCES clients(id),
      prospect_id UUID NOT NULL REFERENCES prospects(id),
      task_id UUID REFERENCES ao_prospect_tasks(id) ON DELETE SET NULL,
      ao_owner_id INTEGER NOT NULL REFERENCES users(id),
      person_spoken_to TEXT,
      role TEXT,
      decision_maker TEXT,
      current_cleaning_solution TEXT,
      stated_context TEXT,
      problem_or_risk TEXT,
      opportunity_timing TEXT
        CHECK (opportunity_timing IS NULL OR opportunity_timing IN ('now', 'later', 'not_at_all')),
      opportunity_type TEXT,
      opportunity_strength TEXT
        CHECK (opportunity_strength IS NULL OR opportunity_strength IN ('strong', 'moderate', 'weak', 'unclear')),
      blocker TEXT,
      recommended_next_step TEXT,
      recommended_message TEXT,
      follow_up_due_at TIMESTAMPTZ,
      next_owner TEXT,
      prescribed_before_diagnosing BOOLEAN,
      real_reason_to_continue BOOLEAN,
      specific_dated_next_step BOOLEAN,
      next_action TEXT
        CHECK (next_action IS NULL OR next_action IN (${DEBRIEF_NEXT_ACTIONS.map(v => `'${v}'`).join(', ')})),
      coaching_feedback TEXT,
      debrief_quality TEXT
        CHECK (debrief_quality IS NULL OR debrief_quality IN (${DEBRIEF_QUALITIES.map(v => `'${v}'`).join(', ')})),
      evaluation JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_prospects_ao_routing
      ON prospects(client_id, assigned_ao_id, prospect_motion)
      WHERE prospect_motion IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_ao_prospect_tasks_owner_deadline
      ON ao_prospect_tasks(assigned_ao_id, client_id, deadline, status)
      WHERE status IN ('open', 'in_progress');
    CREATE INDEX IF NOT EXISTS idx_ao_advisory_debriefs_prospect
      ON ao_advisory_debriefs(prospect_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_ao_advisory_debriefs_owner
      ON ao_advisory_debriefs(ao_owner_id, client_id, created_at DESC);
  `);
}

async function ensureAoProspectRoutingSchema() {
  if (!schemaInitPromise) {
    schemaInitPromise = ensureAoProspectRoutingSchemaOnce().catch(err => {
      schemaInitPromise = null;
      throw err;
    });
  }
  return schemaInitPromise;
}

module.exports = {
  ensureAoProspectRoutingSchema,
};
