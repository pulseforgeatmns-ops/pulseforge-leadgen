-- PROPOSED ONLY: explicit operator-reviewed migration; never run by startup.
BEGIN;
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '30s';
CREATE TABLE governed_outbound_stage_schedule (
 tenant_id text NOT NULL CHECK (tenant_id IN ('1','10','13','17')),
 stage text NOT NULL CHECK (stage IN ('poll','tick','control')),
 due_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 status text NOT NULL DEFAULT 'due' CHECK (status IN ('due','running','retry','held','uncertain')),
 run_id uuid, claim_token uuid, fence bigint NOT NULL DEFAULT 0,
 lease_until timestamptz, execution_started_at timestamptz,
 attempts integer NOT NULL DEFAULT 0 CHECK(attempts >= 0),
 last_completed_at timestamptz, last_outcome text, reason text,
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,stage),
 CHECK ((status='running' AND claim_token IS NOT NULL AND lease_until IS NOT NULL)
 OR (status<>'running' AND claim_token IS NULL AND lease_until IS NULL))
);
CREATE TABLE governed_outbound_stage_runs (
 run_id uuid PRIMARY KEY, tenant_id text NOT NULL, stage text NOT NULL,
 due_at timestamptz NOT NULL, attempt integer NOT NULL, fence bigint NOT NULL,
 status text NOT NULL CHECK(status IN ('claimed','running','completed','held','retry','uncertain')),
 started_at timestamptz, finished_at timestamptz, reason text,
 evidence_refs jsonb NOT NULL DEFAULT '{}'::jsonb CHECK(jsonb_typeof(evidence_refs)='object'),
 UNIQUE(tenant_id,stage,due_at),
 FOREIGN KEY(tenant_id,stage) REFERENCES governed_outbound_stage_schedule(tenant_id,stage)
);
CREATE INDEX governed_outbound_stage_runs_tenant ON governed_outbound_stage_runs(tenant_id,stage,due_at DESC);
COMMIT;
