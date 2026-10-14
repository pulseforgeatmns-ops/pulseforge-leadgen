-- Synthetic disposable test schema only. No production migration or backfill.
CREATE TABLE companies (id uuid PRIMARY KEY, client_id integer NOT NULL,
  name text, domain text, website text, location text, industry text);
CREATE TABLE prospects (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  company_id uuid REFERENCES companies(id), first_name text, last_name text,
  email text UNIQUE, phone text, status text, source text, icp_score integer, notes text, vertical text,
  client_id integer NOT NULL, service_area_match text, discovery_method text, has_website boolean,
  google_review_count integer, google_rating numeric, has_facebook boolean, has_instagram boolean,
  facebook_url text, instagram_url text, website_url text, email_verified boolean,
  email_verification_method text, verified_at timestamptz, do_not_contact boolean DEFAULT false,
  preferred_channel text, email_status text, verifier_response jsonb, verifier_checked_at timestamptz,
  job_title text, acquisition_metadata jsonb NOT NULL DEFAULT '{}', enrichment_provenance jsonb,
  setter_status text, setter_updated_at timestamptz, updated_at timestamptz DEFAULT now(),
  assigned_ao_id integer, closer_id integer, last_contacted_at timestamptz, last_reply_at timestamptz,
  acquisition_knowledge_object_id text
);
CREATE TABLE acquisition_knowledge_objects (id text PRIMARY KEY, tenant_id text, content jsonb,
  evidence jsonb, provenance jsonb, object_type text, lifecycle_state text, validation_state text,
  version integer, relationships jsonb, updated_at timestamptz);
CREATE TABLE ao_prospect_tasks (client_id integer, prospect_id uuid);
CREATE TABLE touchpoints (client_id integer, prospect_id uuid, action_type text);
CREATE TABLE ao_leads (id text, client_id integer, business_name text, crm_prospect_id uuid);
CREATE TABLE ao_contacts (lead_id text, email text);
CREATE TABLE acquisition_outbound_lifecycle (tenant_id text, suppressed boolean, email text, company_id text, state text);
CREATE TABLE acquisition_outbound_items (id text, tenant_id text, attempted_at timestamptz, email text, company_id text);
CREATE TABLE acquisition_mission_outbound_executions (id text, tenant_id text, status text, mission_id text, payload jsonb, prospect_id text);
