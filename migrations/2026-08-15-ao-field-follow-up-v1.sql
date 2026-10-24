-- AO Field Follow-Up V1
-- Adds AO role support, field visit leads, contacts, follow-up tasks, and escalations.

ALTER TABLE users
  ADD COLUMN IF NOT EXISTS phone TEXT,
  ADD COLUMN IF NOT EXISTS territory TEXT,
  ADD COLUMN IF NOT EXISTS manager_id INTEGER REFERENCES users(id),
  ADD COLUMN IF NOT EXISTS daily_goal INTEGER,
  ADD COLUMN IF NOT EXISTS weekly_goal INTEGER;

-- Role constraint updated at runtime via utils/aoFieldSchema.js ensureAoRoleConstraint()

CREATE TABLE IF NOT EXISTS ao_leads (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id),
  business_name TEXT NOT NULL,
  address TEXT,
  business_type TEXT,
  status TEXT NOT NULL DEFAULT 'new_visit',
  interest_level TEXT NOT NULL DEFAULT 'medium',
  ao_owner_id INTEGER NOT NULL REFERENCES users(id),
  first_contact_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_contact_date TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  next_follow_up_date DATE,
  next_follow_up_owner_id INTEGER REFERENCES users(id),
  attribution_source TEXT NOT NULL DEFAULT 'ao_field_visit',
  attribution_window_days INTEGER NOT NULL DEFAULT 180,
  commission_eligible BOOLEAN NOT NULL DEFAULT true,
  original_visit_note TEXT,
  closed_revenue_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

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
);

CREATE TABLE IF NOT EXISTS ao_follow_up_tasks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  lead_id UUID NOT NULL REFERENCES ao_leads(id) ON DELETE CASCADE,
  contact_id UUID REFERENCES ao_contacts(id) ON DELETE SET NULL,
  ao_owner_id INTEGER NOT NULL REFERENCES users(id),
  due_date DATE NOT NULL,
  status TEXT NOT NULL DEFAULT 'open',
  priority TEXT NOT NULL DEFAULT 'normal',
  next_action TEXT,
  last_interaction_summary TEXT,
  suggested_message TEXT,
  waiting_on_jake BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS ao_escalations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  lead_id UUID NOT NULL REFERENCES ao_leads(id) ON DELETE CASCADE,
  contact_id UUID REFERENCES ao_contacts(id) ON DELETE SET NULL,
  ao_owner_id INTEGER NOT NULL REFERENCES users(id),
  reason TEXT NOT NULL,
  summary TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'new',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  resolved_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS ao_max_sessions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  ao_owner_id INTEGER NOT NULL REFERENCES users(id),
  client_id INTEGER NOT NULL REFERENCES clients(id),
  mode TEXT NOT NULL,
  step_index INTEGER NOT NULL DEFAULT 0,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  completed BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ao_leads_owner ON ao_leads(ao_owner_id, client_id);
CREATE INDEX IF NOT EXISTS idx_ao_leads_next_follow_up ON ao_leads(next_follow_up_date);
CREATE INDEX IF NOT EXISTS idx_ao_tasks_owner_due ON ao_follow_up_tasks(ao_owner_id, due_date, status);
CREATE INDEX IF NOT EXISTS idx_ao_escalations_status ON ao_escalations(status, created_at DESC);
