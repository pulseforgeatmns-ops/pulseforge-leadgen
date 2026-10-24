-- SPEC-223A Canonical Semantic Persistence Schema
-- Additive, append-only canonical semantic substrate. No interpreter/projector runtime.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER TABLE cie_evidence
  ADD COLUMN IF NOT EXISTS source_text_sha256 CHAR(64),
  ADD COLUMN IF NOT EXISTS immutable_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS canonical_registry_artifacts (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  registry_version TEXT NOT NULL,
  entity_vocabulary JSONB NOT NULL,
  predicate_definitions JSONB NOT NULL,
  content_digest CHAR(64) NOT NULL UNIQUE CHECK (content_digest ~ '^[0-9a-f]{64}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (registry_version, content_digest)
);

CREATE TABLE IF NOT EXISTS canonical_interpretation_batches (
  tenant_id TEXT NOT NULL,
  id UUID NOT NULL DEFAULT gen_random_uuid(),
  registry_artifact_id UUID NOT NULL REFERENCES canonical_registry_artifacts(id),
  registry_version TEXT NOT NULL,
  registry_content_digest CHAR(64) NOT NULL CHECK (registry_content_digest ~ '^[0-9a-f]{64}$'),
  interpreter_id TEXT NOT NULL,
  interpreter_version TEXT NOT NULL,
  semantic_model_version INTEGER NOT NULL,
  idempotency_key CHAR(64) NOT NULL CHECK (idempotency_key ~ '^[0-9a-f]{64}$'),
  status TEXT NOT NULL CHECK (status IN ('COMMITTED', 'REJECTED')),
  diagnostics JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  committed_at TIMESTAMPTZ,
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS canonical_business_entities (
  tenant_id TEXT NOT NULL,
  id UUID NOT NULL DEFAULT gen_random_uuid(),
  entity_type TEXT NOT NULL CHECK (entity_type IN (
    'BUSINESS', 'OFFER', 'PROGRAM', 'CUSTOMER_PROFILE', 'PAIN',
    'CAPABILITY', 'OUTCOME', 'OBJECTIVE', 'METRIC'
  )),
  identity_key TEXT NOT NULL,
  domain_client_id INTEGER REFERENCES clients(id),
  created_by_batch_id UUID NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, entity_type, identity_key),
  FOREIGN KEY (tenant_id, created_by_batch_id)
    REFERENCES canonical_interpretation_batches (tenant_id, id),
  CHECK (
    (entity_type = 'BUSINESS' AND domain_client_id IS NOT NULL)
    OR (entity_type <> 'BUSINESS' AND domain_client_id IS NULL)
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS canonical_one_business_per_client_idx
  ON canonical_business_entities (domain_client_id)
  WHERE entity_type = 'BUSINESS';

CREATE TABLE IF NOT EXISTS canonical_entity_label_assertions (
  tenant_id TEXT NOT NULL,
  id UUID NOT NULL DEFAULT gen_random_uuid(),
  entity_id UUID NOT NULL,
  label TEXT NOT NULL,
  assertion_kind TEXT NOT NULL CHECK (assertion_kind IN ('CANONICAL', 'ALIAS')),
  evidence_id UUID,
  created_by_batch_id UUID NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, id),
  FOREIGN KEY (tenant_id, entity_id)
    REFERENCES canonical_business_entities (tenant_id, id),
  FOREIGN KEY (tenant_id, created_by_batch_id)
    REFERENCES canonical_interpretation_batches (tenant_id, id),
  UNIQUE (tenant_id, entity_id, label, assertion_kind)
);

CREATE TABLE IF NOT EXISTS canonical_entity_merge_events (
  tenant_id TEXT NOT NULL,
  id UUID NOT NULL DEFAULT gen_random_uuid(),
  source_entity_id UUID NOT NULL,
  target_entity_id UUID NOT NULL,
  event_kind TEXT NOT NULL CHECK (event_kind IN ('MERGED', 'MERGE_REVOKED', 'SPLIT_SUCCESSOR')),
  evidence_id UUID,
  created_by_batch_id UUID NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, id),
  FOREIGN KEY (tenant_id, source_entity_id)
    REFERENCES canonical_business_entities (tenant_id, id),
  FOREIGN KEY (tenant_id, target_entity_id)
    REFERENCES canonical_business_entities (tenant_id, id),
  FOREIGN KEY (tenant_id, created_by_batch_id)
    REFERENCES canonical_interpretation_batches (tenant_id, id),
  CHECK (source_entity_id <> target_entity_id)
);

CREATE TABLE IF NOT EXISTS canonical_batch_evidence_inputs (
  tenant_id TEXT NOT NULL,
  batch_id UUID NOT NULL,
  evidence_id UUID NOT NULL REFERENCES cie_evidence(id),
  source_text_sha256 CHAR(64) NOT NULL CHECK (source_text_sha256 ~ '^[0-9a-f]{64}$'),
  input_ordinal INTEGER NOT NULL CHECK (input_ordinal >= 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, batch_id, evidence_id),
  UNIQUE (tenant_id, batch_id, input_ordinal),
  FOREIGN KEY (tenant_id, batch_id)
    REFERENCES canonical_interpretation_batches (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS canonical_conflict_sets (
  tenant_id TEXT NOT NULL,
  id UUID NOT NULL DEFAULT gen_random_uuid(),
  semantic_slot_key CHAR(64) NOT NULL CHECK (semantic_slot_key ~ '^[0-9a-f]{64}$'),
  predicate TEXT NOT NULL,
  subject_entity_id UUID NOT NULL,
  slot_cardinality TEXT NOT NULL CHECK (slot_cardinality IN
    ('SINGLE', 'OPTIONAL_SINGLE', 'SET', 'ORDERED_SET')),
  created_by_batch_id UUID NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, semantic_slot_key),
  FOREIGN KEY (tenant_id, subject_entity_id)
    REFERENCES canonical_business_entities (tenant_id, id),
  FOREIGN KEY (tenant_id, created_by_batch_id)
    REFERENCES canonical_interpretation_batches (tenant_id, id)
);

CREATE TABLE IF NOT EXISTS canonical_business_facts (
  tenant_id TEXT NOT NULL,
  id UUID NOT NULL DEFAULT gen_random_uuid(),
  proposition_key CHAR(64) NOT NULL CHECK (proposition_key ~ '^[0-9a-f]{64}$'),
  assertion_key CHAR(64) NOT NULL CHECK (assertion_key ~ '^[0-9a-f]{64}$'),
  subject_entity_id UUID NOT NULL,
  predicate TEXT NOT NULL,
  object_value JSONB NOT NULL,
  qualifiers JSONB NOT NULL DEFAULT '{}'::jsonb,
  epistemic_state TEXT NOT NULL CHECK (epistemic_state IN
    ('KNOWN', 'HYPOTHESIS', 'UNKNOWN', 'UNRESOLVED', 'NOT_APPLICABLE')),
  epistemic_confidence NUMERIC(4,3) CHECK (epistemic_confidence BETWEEN 0 AND 1),
  epistemic_calibration_version TEXT,
  interpretation_confidence NUMERIC(4,3) NOT NULL CHECK (interpretation_confidence BETWEEN 0 AND 1),
  interpretation_calibration_version TEXT NOT NULL,
  temporal_status TEXT NOT NULL CHECK (temporal_status IN ('CURRENT', 'PLANNED', 'HISTORICAL', 'RETIRED')),
  valid_from TIMESTAMPTZ,
  valid_to TIMESTAMPTZ,
  modality TEXT NOT NULL CHECK (modality IN ('ACTUAL', 'INTENDED', 'CONDITIONAL')),
  conflict_set_id UUID NOT NULL,
  created_by_batch_id UUID NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, assertion_key),
  FOREIGN KEY (tenant_id, subject_entity_id)
    REFERENCES canonical_business_entities (tenant_id, id),
  FOREIGN KEY (tenant_id, conflict_set_id)
    REFERENCES canonical_conflict_sets (tenant_id, id),
  FOREIGN KEY (tenant_id, created_by_batch_id)
    REFERENCES canonical_interpretation_batches (tenant_id, id),
  CHECK (valid_from IS NULL OR valid_to IS NULL OR valid_from <= valid_to),
  CHECK (temporal_status <> 'PLANNED' OR modality IN ('INTENDED', 'CONDITIONAL'))
);

CREATE TABLE IF NOT EXISTS canonical_fact_evidence_refs (
  tenant_id TEXT NOT NULL,
  id UUID NOT NULL DEFAULT gen_random_uuid(),
  fact_id UUID NOT NULL,
  evidence_id UUID NOT NULL REFERENCES cie_evidence(id),
  source_text_sha256 CHAR(64) NOT NULL CHECK (source_text_sha256 ~ '^[0-9a-f]{64}$'),
  span_start_utf16 INTEGER NOT NULL,
  span_end_utf16 INTEGER NOT NULL,
  support_type TEXT NOT NULL CHECK (support_type IN ('DIRECT', 'INFERRED', 'OPERATOR_CONFIRMED')),
  created_by_batch_id UUID NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, fact_id, evidence_id, span_start_utf16, span_end_utf16),
  FOREIGN KEY (tenant_id, fact_id)
    REFERENCES canonical_business_facts (tenant_id, id),
  FOREIGN KEY (tenant_id, created_by_batch_id)
    REFERENCES canonical_interpretation_batches (tenant_id, id),
  CHECK (span_start_utf16 >= 0 AND span_end_utf16 > span_start_utf16)
);

CREATE TABLE IF NOT EXISTS canonical_fact_relations (
  tenant_id TEXT NOT NULL,
  id UUID NOT NULL DEFAULT gen_random_uuid(),
  from_fact_id UUID NOT NULL,
  relation_type TEXT NOT NULL CHECK (relation_type IN
    ('SUPERSEDES', 'CORRECTION_OF', 'CONTRADICTS', 'DEPENDS_ON')),
  to_fact_id UUID NOT NULL,
  created_by_batch_id UUID NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, from_fact_id, relation_type, to_fact_id),
  FOREIGN KEY (tenant_id, from_fact_id)
    REFERENCES canonical_business_facts (tenant_id, id),
  FOREIGN KEY (tenant_id, to_fact_id)
    REFERENCES canonical_business_facts (tenant_id, id),
  FOREIGN KEY (tenant_id, created_by_batch_id)
    REFERENCES canonical_interpretation_batches (tenant_id, id),
  CHECK (from_fact_id <> to_fact_id)
);

CREATE TABLE IF NOT EXISTS canonical_business_snapshots (
  tenant_id TEXT NOT NULL,
  id UUID NOT NULL DEFAULT gen_random_uuid(),
  committed_batch_id UUID NOT NULL,
  registry_artifact_id UUID NOT NULL REFERENCES canonical_registry_artifacts(id),
  registry_version TEXT NOT NULL,
  registry_content_digest CHAR(64) NOT NULL CHECK (registry_content_digest ~ '^[0-9a-f]{64}$'),
  supersedes_snapshot_id UUID,
  manifest JSONB NOT NULL,
  manifest_digest CHAR(64) NOT NULL UNIQUE CHECK (manifest_digest ~ '^[0-9a-f]{64}$'),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, id),
  UNIQUE (tenant_id, committed_batch_id),
  FOREIGN KEY (tenant_id, committed_batch_id)
    REFERENCES canonical_interpretation_batches (tenant_id, id),
  FOREIGN KEY (tenant_id, supersedes_snapshot_id)
    REFERENCES canonical_business_snapshots (tenant_id, id)
);

CREATE OR REPLACE FUNCTION canonical_assert_evidence_tenant()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE expected_tenant TEXT;
BEGIN
  SELECT tw.tenant_key INTO expected_tenant
  FROM cie_evidence evidence
  JOIN tenant_workspaces tw ON tw.client_id = evidence.client_id
  WHERE evidence.id = NEW.evidence_id;
  IF expected_tenant IS NULL OR expected_tenant <> NEW.tenant_id THEN
    RAISE EXCEPTION 'canonical evidence reference crosses tenant boundary';
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION canonical_assert_business_domain_binding()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE expected_tenant TEXT;
BEGIN
  IF NEW.entity_type <> 'BUSINESS' THEN RETURN NEW; END IF;
  SELECT tenant_key INTO expected_tenant FROM tenant_workspaces WHERE client_id = NEW.domain_client_id;
  IF expected_tenant IS NULL OR expected_tenant <> NEW.tenant_id THEN
    RAISE EXCEPTION 'canonical BUSINESS must bind to its client tenant workspace';
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION canonical_assert_merge_entity_types()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE source_type TEXT;
DECLARE target_type TEXT;
BEGIN
  SELECT entity_type INTO source_type FROM canonical_business_entities
  WHERE tenant_id = NEW.tenant_id AND id = NEW.source_entity_id;
  SELECT entity_type INTO target_type FROM canonical_business_entities
  WHERE tenant_id = NEW.tenant_id AND id = NEW.target_entity_id;
  IF source_type IS NULL OR target_type IS NULL OR source_type <> target_type THEN
    RAISE EXCEPTION 'canonical merge endpoints must be same-tenant and same-type';
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION canonical_verify_registry_artifact()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.content_digest <> encode(digest(jsonb_build_object(
    'entity_vocabulary', NEW.entity_vocabulary,
    'predicate_definitions', NEW.predicate_definitions,
    'registry_version', NEW.registry_version
  )::text, 'sha256'), 'hex') THEN
    RAISE EXCEPTION 'canonical registry artifact digest mismatch';
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION canonical_verify_snapshot_manifest()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE artifact canonical_registry_artifacts%ROWTYPE;
BEGIN
  SELECT * INTO artifact FROM canonical_registry_artifacts WHERE id = NEW.registry_artifact_id;
  IF artifact.id IS NULL OR artifact.registry_version <> NEW.registry_version
    OR artifact.content_digest <> NEW.registry_content_digest THEN
    RAISE EXCEPTION 'canonical snapshot registry artifact mismatch';
  END IF;
  IF NEW.manifest_digest <> encode(digest(NEW.manifest::text, 'sha256'), 'hex') THEN
    RAISE EXCEPTION 'canonical snapshot manifest digest mismatch';
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION canonical_assert_batch_registry()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE artifact canonical_registry_artifacts%ROWTYPE;
BEGIN
  SELECT * INTO artifact FROM canonical_registry_artifacts WHERE id = NEW.registry_artifact_id;
  IF artifact.id IS NULL OR artifact.registry_version <> NEW.registry_version
    OR artifact.content_digest <> NEW.registry_content_digest THEN
    RAISE EXCEPTION 'canonical batch registry artifact mismatch';
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION canonical_reject_semantic_mutation()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION '% is append-only', TG_TABLE_NAME;
END;
$$;

CREATE OR REPLACE FUNCTION canonical_reject_immutable_evidence_mutation()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.immutable_at IS NOT NULL
    AND (NEW.statement IS DISTINCT FROM OLD.statement
      OR NEW.source_text_sha256 IS DISTINCT FROM OLD.source_text_sha256) THEN
    RAISE EXCEPTION 'immutable CIE evidence cannot be changed';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS canonical_business_domain_binding_trigger ON canonical_business_entities;
CREATE TRIGGER canonical_business_domain_binding_trigger
  BEFORE INSERT ON canonical_business_entities
  FOR EACH ROW EXECUTE FUNCTION canonical_assert_business_domain_binding();

DROP TRIGGER IF EXISTS canonical_merge_entity_types_trigger ON canonical_entity_merge_events;
CREATE TRIGGER canonical_merge_entity_types_trigger
  BEFORE INSERT ON canonical_entity_merge_events
  FOR EACH ROW EXECUTE FUNCTION canonical_assert_merge_entity_types();

DROP TRIGGER IF EXISTS canonical_batch_registry_trigger ON canonical_interpretation_batches;
CREATE TRIGGER canonical_batch_registry_trigger
  BEFORE INSERT ON canonical_interpretation_batches
  FOR EACH ROW EXECUTE FUNCTION canonical_assert_batch_registry();

DROP TRIGGER IF EXISTS canonical_batch_evidence_tenant_trigger ON canonical_batch_evidence_inputs;
CREATE TRIGGER canonical_batch_evidence_tenant_trigger
  BEFORE INSERT ON canonical_batch_evidence_inputs
  FOR EACH ROW EXECUTE FUNCTION canonical_assert_evidence_tenant();

DROP TRIGGER IF EXISTS canonical_fact_evidence_tenant_trigger ON canonical_fact_evidence_refs;
CREATE TRIGGER canonical_fact_evidence_tenant_trigger
  BEFORE INSERT ON canonical_fact_evidence_refs
  FOR EACH ROW EXECUTE FUNCTION canonical_assert_evidence_tenant();

DROP TRIGGER IF EXISTS canonical_label_evidence_tenant_trigger ON canonical_entity_label_assertions;
CREATE TRIGGER canonical_label_evidence_tenant_trigger
  BEFORE INSERT ON canonical_entity_label_assertions
  FOR EACH ROW WHEN (NEW.evidence_id IS NOT NULL)
  EXECUTE FUNCTION canonical_assert_evidence_tenant();

DROP TRIGGER IF EXISTS canonical_merge_evidence_tenant_trigger ON canonical_entity_merge_events;
CREATE TRIGGER canonical_merge_evidence_tenant_trigger
  BEFORE INSERT ON canonical_entity_merge_events
  FOR EACH ROW WHEN (NEW.evidence_id IS NOT NULL)
  EXECUTE FUNCTION canonical_assert_evidence_tenant();

DROP TRIGGER IF EXISTS canonical_registry_digest_trigger ON canonical_registry_artifacts;
CREATE TRIGGER canonical_registry_digest_trigger
  BEFORE INSERT ON canonical_registry_artifacts
  FOR EACH ROW EXECUTE FUNCTION canonical_verify_registry_artifact();

DROP TRIGGER IF EXISTS canonical_snapshot_digest_trigger ON canonical_business_snapshots;
CREATE TRIGGER canonical_snapshot_digest_trigger
  BEFORE INSERT ON canonical_business_snapshots
  FOR EACH ROW EXECUTE FUNCTION canonical_verify_snapshot_manifest();

DROP TRIGGER IF EXISTS canonical_cie_evidence_immutable_trigger ON cie_evidence;
CREATE TRIGGER canonical_cie_evidence_immutable_trigger
  BEFORE UPDATE ON cie_evidence
  FOR EACH ROW EXECUTE FUNCTION canonical_reject_immutable_evidence_mutation();

DO $$
DECLARE table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'canonical_registry_artifacts', 'canonical_interpretation_batches',
    'canonical_business_entities', 'canonical_entity_label_assertions',
    'canonical_entity_merge_events', 'canonical_batch_evidence_inputs',
    'canonical_conflict_sets', 'canonical_business_facts',
    'canonical_fact_evidence_refs', 'canonical_fact_relations',
    'canonical_business_snapshots'
  ] LOOP
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I', table_name || '_append_only', table_name);
    EXECUTE format('CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION canonical_reject_semantic_mutation()', table_name || '_append_only', table_name);
  END LOOP;
END;
$$;