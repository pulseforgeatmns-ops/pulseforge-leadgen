
CREATE TABLE clients(id integer PRIMARY KEY);
CREATE TABLE users(id integer PRIMARY KEY,client_id integer REFERENCES clients(id),name text,email text,role text,active boolean DEFAULT true,UNIQUE(client_id,id));
CREATE TABLE companies(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),client_id integer REFERENCES clients(id),name text);
CREATE TABLE prospects(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),client_id integer REFERENCES clients(id),company_id uuid REFERENCES companies(id),assigned_ao_id integer REFERENCES users(id),first_name text,last_name text,email text,phone text,job_title text,ao_current_status text,status text,source text,acquisition_metadata jsonb,ao_last_touch_at timestamptz,UNIQUE(client_id,id));
CREATE TABLE ao_prospect_activity(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),prospect_id uuid,tenant_id integer,ao_id integer,activity_type text,notes text,metadata jsonb,created_at timestamptz DEFAULT now(),FOREIGN KEY(tenant_id,prospect_id) REFERENCES prospects(client_id,id));

CREATE TABLE touchpoints(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),client_id integer,prospect_id uuid,channel text,action_type text,outcome text,content_summary text,created_at timestamptz DEFAULT now());
CREATE TABLE IF NOT EXISTS ao_prospect_tasks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id),
  prospect_id UUID NOT NULL,
  assigned_ao_id INTEGER NOT NULL,
  assignment_category TEXT NOT NULL
    CHECK (assignment_category IN (
      'UNFAIR_ADVANTAGE', 'HIGH_VALUE_ICP', 'ROUTE_CLUSTER',
      'WALK_IN_OPPORTUNITY', 'WARM_SIGNAL', 'FOLLOW_UP_REQUIRED'
    )),
  motion TEXT NOT NULL CHECK (motion IN ('EMAIL_LED', 'AO_LED', 'HYBRID', 'NURTURE')),
  priority TEXT NOT NULL DEFAULT 'normal' CHECK (priority IN ('normal', 'high', 'warm')),
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
);

CREATE TABLE max_ao_follow_up_tasks(id text PRIMARY KEY,client_id integer,prospect_id text,owner_id integer,prompt text,status text,created_at timestamptz DEFAULT now());
CREATE TABLE tenant_outreach_suppressions(id text PRIMARY KEY,tenant_id text,contact_ref text,email text,reason text,source text,status text,metadata jsonb,revoked_at timestamptz,created_at timestamptz DEFAULT now());

CREATE TABLE prospect_notes(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),client_id integer,prospect_id uuid,text text,created_at timestamptz DEFAULT now());
CREATE TABLE prospect_lifecycle_events(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),client_id integer,prospect_id uuid,reason text,created_at timestamptz DEFAULT now());

CREATE TABLE ao_leads(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),client_id integer,ao_owner_id integer,crm_prospect_id uuid,original_visit_note text);
CREATE TABLE ao_contacts(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),lead_id uuid,contact_name text,contact_title text,email text,phone text);
CREATE TABLE ao_follow_up_tasks(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),lead_id uuid,ao_owner_id integer,next_action text,due_date date,status text DEFAULT 'open');
