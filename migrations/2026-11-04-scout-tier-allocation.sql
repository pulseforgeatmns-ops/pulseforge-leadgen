-- SPEC-SCOUT-P1-001 Rev. 2 (#1048). Tenant 10 only. Apply separately through
-- the approved migration workflow; this PR does not apply it.
-- Additive: no existing table, row, trigger or query history is changed.
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='30s';

-- Allocation anchor. Counters are derived from scout_query_pages created at or
-- after activated_at; earlier searches inform novelty only. Inserting the
-- activation row is a separate, explicitly approved production write.
CREATE TABLE scout_query_allocation_activations (
  tenant_id text NOT NULL CHECK (tenant_id='10'),
  allocation_version text NOT NULL CHECK (allocation_version ~ '^[a-z0-9][a-z0-9-]{2,63}$'),
  activated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  approval_ref text NOT NULL CHECK (length(btrim(approval_ref)) BETWEEN 1 AND 512),
  PRIMARY KEY (tenant_id, allocation_version)
);

-- Commercial qualification. No row means unknown. This PR has no writer;
-- the rubric and the human-review write path are defined in a separate spec.
CREATE TABLE scout_candidate_commercial_qualifications (
  research_candidate_id bigint PRIMARY KEY REFERENCES scout_research_candidates(id),
  tenant_id text NOT NULL CHECK (tenant_id='10'),
  state text NOT NULL CHECK (state IN ('qualified','not_qualified')),
  source text NOT NULL CHECK (source IN ('verified_evidence','human_review')),
  actor text NOT NULL CHECK (length(btrim(actor)) BETWEEN 1 AND 256),
  rubric_version text NOT NULL CHECK (length(btrim(rubric_version)) BETWEEN 1 AND 128),
  evidence_refs jsonb NOT NULL CHECK (jsonb_typeof(evidence_refs)='array' AND jsonb_array_length(evidence_refs)>0),
  decided_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION scout_tier_allocation_permanent() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'scout_tier_allocation_permanent_record'; END $$;
CREATE TRIGGER scout_query_allocation_activation_permanent
  BEFORE UPDATE OR DELETE OR TRUNCATE ON scout_query_allocation_activations
  FOR EACH STATEMENT EXECUTE FUNCTION scout_tier_allocation_permanent();
CREATE TRIGGER scout_candidate_qualification_permanent
  BEFORE UPDATE OR DELETE OR TRUNCATE ON scout_candidate_commercial_qualifications
  FOR EACH STATEMENT EXECUTE FUNCTION scout_tier_allocation_permanent();

CREATE INDEX scout_anchor_page_created_lookup
  ON scout_query_pages(tenant_id, created_at) WHERE tenant_id='10';
COMMIT;
