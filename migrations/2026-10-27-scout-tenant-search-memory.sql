-- Anchor discovery memory only. Apply separately while research remains paused.
-- Existing source fingerprints, checkpoints, tickets and grants are unchanged.
BEGIN;
SET LOCAL lock_timeout='5s';
SET LOCAL statement_timeout='30s';

CREATE TABLE scout_tenant_query_owners (
  tenant_id text NOT NULL CHECK(tenant_id='10'),
  query_key text NOT NULL CHECK(query_key ~ '^[a-f0-9]{64}$'),
  root_query_key text NOT NULL CHECK(root_query_key ~ '^[a-f0-9]{64}$'),
  query_text text NOT NULL CHECK(length(query_text) BETWEEN 1 AND 511),
  search_fingerprint text NOT NULL CHECK(length(search_fingerprint)>0),
  claimed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY(tenant_id,query_key)
);

CREATE TABLE scout_tenant_place_observations (
  tenant_id text NOT NULL CHECK(tenant_id='10'),
  place_id text NOT NULL CHECK(length(place_id) BETWEEN 1 AND 512),
  search_fingerprint text NOT NULL CHECK(length(search_fingerprint)>0),
  reservation_id uuid NOT NULL UNIQUE,
  state text NOT NULL CHECK(state IN ('reserved','observed','uncertain')),
  candidate jsonb,
  claimed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  observed_at timestamptz,
  PRIMARY KEY(tenant_id,place_id),
  CHECK(coalesce((state='observed' AND candidate IS NOT NULL AND observed_at IS NOT NULL
    AND jsonb_typeof(candidate)='object' AND candidate->>'placeId'=place_id)
    OR (state IN ('reserved','uncertain') AND candidate IS NULL AND observed_at IS NULL),false))
);

CREATE INDEX scout_anchor_checkpoint_query_lookup
  ON scout_query_checkpoints(tenant_id,query_key,plan_version) WHERE tenant_id='10';
CREATE INDEX scout_anchor_page_root_lookup
  ON scout_query_pages(tenant_id,root_query_key) WHERE tenant_id='10';
CREATE INDEX scout_anchor_member_place_lookup
  ON scout_candidate_memberships(tenant_id,(raw_candidate->>'placeId')) WHERE tenant_id='10';

CREATE FUNCTION scout_tenant_query_owner_permanent() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'scout_tenant_query_owner_permanent'; END $$;
CREATE TRIGGER scout_tenant_query_owner_permanent
  BEFORE UPDATE OR DELETE OR TRUNCATE ON scout_tenant_query_owners
  FOR EACH STATEMENT EXECUTE FUNCTION scout_tenant_query_owner_permanent();
CREATE FUNCTION scout_tenant_place_observation_once() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.state<>'reserved' OR NEW.state NOT IN ('observed','uncertain')
    OR (to_jsonb(NEW)-ARRAY['state','candidate','observed_at'])
      IS DISTINCT FROM (to_jsonb(OLD)-ARRAY['state','candidate','observed_at'])
  THEN RAISE EXCEPTION 'scout_tenant_place_observation_permanent'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER scout_tenant_place_observation_once
  BEFORE UPDATE ON scout_tenant_place_observations
  FOR EACH ROW EXECUTE FUNCTION scout_tenant_place_observation_once();
CREATE TRIGGER scout_tenant_place_observation_no_delete
  BEFORE DELETE OR TRUNCATE ON scout_tenant_place_observations
  FOR EACH STATEMENT EXECUTE FUNCTION scout_tenant_query_owner_permanent();
COMMIT;
