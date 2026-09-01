/**
 * SPEC-224 -- Blueprint <-> Canonical Snapshot Association
 *
 * canonical_business_snapshots is tenant-partitioned with a composite
 * PRIMARY KEY (tenant_id, id) -- there is no single-column unique key on
 * id alone, so the Blueprint side must store BOTH the tenant_id and the
 * snapshot id and reference them together.
 *
 * cie_business_blueprints has no tenant_id column (only client_id); the
 * tenant_id is resolved at approval time from tenant_workspaces and stored
 * alongside the snapshot id so the FK stays meaningful without requiring a
 * schema change to the client-scoped Blueprint table's identity.
 */

async function up(pool) {
  await pool.query(`
    ALTER TABLE cie_business_blueprints
    ADD COLUMN IF NOT EXISTS canonical_snapshot_tenant_id TEXT NULL,
    ADD COLUMN IF NOT EXISTS canonical_snapshot_id UUID NULL
  `);

  await pool.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'cie_bp_canonical_snapshot_fk'
      ) THEN
        ALTER TABLE cie_business_blueprints
        ADD CONSTRAINT cie_bp_canonical_snapshot_fk
        FOREIGN KEY (canonical_snapshot_tenant_id, canonical_snapshot_id)
        REFERENCES canonical_business_snapshots (tenant_id, id)
        ON DELETE RESTRICT ON UPDATE CASCADE;
      END IF;
    END;
    $$;
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_cie_bp_canonical_snapshot
    ON cie_business_blueprints (canonical_snapshot_tenant_id, canonical_snapshot_id)
    WHERE canonical_snapshot_id IS NOT NULL
  `);

  console.log('[SPEC-224] Blueprint <-> canonical snapshot association ready');
}

async function down(pool) {
  await pool.query(`DROP INDEX IF EXISTS idx_cie_bp_canonical_snapshot`);
  await pool.query(`ALTER TABLE cie_business_blueprints DROP CONSTRAINT IF EXISTS cie_bp_canonical_snapshot_fk`);
  await pool.query(`
    ALTER TABLE cie_business_blueprints
    DROP COLUMN IF EXISTS canonical_snapshot_id,
    DROP COLUMN IF EXISTS canonical_snapshot_tenant_id
  `);
}

module.exports = { up, down };
