BEGIN;
-- Apply separately through the approved migration workflow. No runtime DDL.
-- Query payloads remain authoritative; this migration never clears them.
CREATE TABLE IF NOT EXISTS scout_candidate_work (
  tenant_id text NOT NULL,
  search_fingerprint text NOT NULL,
  plan_version text NOT NULL,
  ticket_version text NOT NULL,
  candidate_key text NOT NULL,
  state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','deferred','blocked','handled')),
  reason text,
  dependency_key text,
  membership_revision bigint NOT NULL DEFAULT 0 CHECK (membership_revision >= 0),
  next_attempt_at timestamptz DEFAULT now(),
  claim_token uuid,
  lease_until timestamptz,
  claim_revision bigint,
  attempts bigint NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  outcome_evidence jsonb,
  last_completion_token uuid,
  last_completion_outcome text,
  last_completion_revision bigint,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id,search_fingerprint,plan_version,ticket_version,candidate_key),
  CHECK ((claim_token IS NULL AND lease_until IS NULL AND claim_revision IS NULL)
    OR (claim_token IS NOT NULL AND lease_until IS NOT NULL AND claim_revision IS NOT NULL)),
  CHECK ((last_completion_token IS NULL AND last_completion_outcome IS NULL AND last_completion_revision IS NULL)
    OR (last_completion_token IS NOT NULL AND last_completion_outcome IS NOT NULL AND last_completion_revision IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS scout_candidate_memberships (
  tenant_id text NOT NULL,
  search_fingerprint text NOT NULL,
  plan_version text NOT NULL,
  ticket_version text NOT NULL,
  query_key text NOT NULL,
  payload_hash text NOT NULL,
  ordinal integer NOT NULL CHECK (ordinal >= 0),
  member_key text NOT NULL,
  candidate_key text NOT NULL,
  raw_candidate jsonb NOT NULL CHECK (jsonb_typeof(raw_candidate) = 'object'),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id,search_fingerprint,plan_version,ticket_version,query_key,payload_hash,ordinal),
  UNIQUE (tenant_id,search_fingerprint,plan_version,ticket_version,member_key),
  FOREIGN KEY (tenant_id,search_fingerprint,plan_version,ticket_version,candidate_key)
    REFERENCES scout_candidate_work (tenant_id,search_fingerprint,plan_version,ticket_version,candidate_key),
  FOREIGN KEY (tenant_id,search_fingerprint,query_key)
    REFERENCES scout_query_checkpoints (tenant_id,search_fingerprint,query_key)
);

-- Mutable current work, rather than an ever-growing event-history scan.
CREATE INDEX IF NOT EXISTS scout_candidate_work_due_idx
  ON scout_candidate_work (tenant_id,search_fingerprint,plan_version,ticket_version,next_attempt_at,candidate_key)
  WHERE state IN ('pending','deferred');
CREATE INDEX IF NOT EXISTS scout_candidate_work_blocked_idx
  ON scout_candidate_work (tenant_id,search_fingerprint,plan_version,ticket_version,dependency_key)
  WHERE state='blocked';
CREATE INDEX IF NOT EXISTS scout_candidate_memberships_work_idx
  ON scout_candidate_memberships (tenant_id,search_fingerprint,plan_version,ticket_version,candidate_key);

-- A draining old binary can still acknowledge a query after its responsibility
-- has transferred. Retain that immutable payload across mixed-version overlap;
-- historical payloads already cleared before transfer cannot be reconstructed.
CREATE OR REPLACE FUNCTION scout_preserve_transferred_query_payload() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.candidates IS DISTINCT FROM OLD.candidates AND EXISTS (
    SELECT 1 FROM scout_candidate_memberships m
    WHERE m.tenant_id=OLD.tenant_id AND m.search_fingerprint=OLD.search_fingerprint
      AND m.query_key=OLD.query_key
  ) THEN
    NEW.candidates := OLD.candidates;
  END IF;
  RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS scout_preserve_transferred_payload ON scout_query_checkpoints;
CREATE TRIGGER scout_preserve_transferred_payload BEFORE UPDATE OF candidates ON scout_query_checkpoints
  FOR EACH ROW EXECUTE FUNCTION scout_preserve_transferred_query_payload();

COMMIT;
