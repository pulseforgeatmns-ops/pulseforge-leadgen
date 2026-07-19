'use strict';

const DISPOSITION_VALUES = Object.freeze([
  'voicemail',
  'answered_interested',
  'answered_not_interested',
  'answered_callback',
  'no_answer',
  'wrong_number',
  'disconnected',
  'gatekeeper_relayed',
  'gatekeeper_blocked',
  'incumbent_all_set',
  'qualified',
  'disqualified',
]);

const DISPOSITION_SET = new Set(DISPOSITION_VALUES);

let phase3dPresentCache = null;

function resetPhase3dSchemaCache() {
  phase3dPresentCache = null;
}

async function isPhase3dSetterSchemaPresent(db) {
  if (phase3dPresentCache !== null) return phase3dPresentCache;
  const result = await db.query(`
    SELECT EXISTS (
      SELECT 1
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'prospects'
        AND column_name = 'is_synthetic'
    ) AS present
  `);
  phase3dPresentCache = Boolean(result.rows[0]?.present);
  return phase3dPresentCache;
}

async function notSyntheticSql(db, columnRef = 'is_synthetic') {
  if (!(await isPhase3dSetterSchemaPresent(db))) return 'TRUE';
  return `COALESCE(${columnRef}, false) = false`;
}

async function ensureLegacyCallDispositionSchema(db) {
  await db.query(`
    CREATE TABLE IF NOT EXISTS call_dispositions (
      id SERIAL PRIMARY KEY,
      prospect_id UUID REFERENCES prospects(id),
      client_id INTEGER,
      call_duration_seconds INTEGER,
      disposition TEXT,
      notes TEXT,
      cal_queue_id INTEGER,
      setter_id INTEGER,
      source TEXT NOT NULL DEFAULT 'cal',
      callback_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);
  await db.query(`
    ALTER TABLE call_dispositions
      ADD COLUMN IF NOT EXISTS setter_id INTEGER,
      ADD COLUMN IF NOT EXISTS source TEXT,
      ADD COLUMN IF NOT EXISTS callback_at TIMESTAMPTZ
  `);
  await db.query(`
    UPDATE call_dispositions
    SET source = 'cal'
    WHERE source IS NULL
  `);
  await db.query(`
    ALTER TABLE call_dispositions
      ALTER COLUMN source SET DEFAULT 'cal',
      ALTER COLUMN source SET NOT NULL
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS call_dispositions_client_created_idx
      ON call_dispositions (client_id, created_at DESC)
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS call_dispositions_prospect_idx
      ON call_dispositions (prospect_id, created_at DESC)
  `);
}

async function reconcilePhase3dSetterSchema(db) {
  // Idempotent only. Never creates Phase 3D objects from a blank production schema.
  // Controlled migration migrations/2026-07-19-setter-pilot-quality-control.sql owns forward apply.
  await db.query(`
    ALTER TABLE prospects
      ADD COLUMN IF NOT EXISTS is_synthetic BOOLEAN NOT NULL DEFAULT false,
      ADD COLUMN IF NOT EXISTS synthetic_label TEXT,
      ADD COLUMN IF NOT EXISTS callback_completed_at TIMESTAMPTZ
  `);
  await db.query(`UPDATE prospects SET do_not_contact = true WHERE is_synthetic = true`);
  await db.query(`
    CREATE OR REPLACE FUNCTION enforce_synthetic_prospect_suppression()
    RETURNS trigger AS $$
    BEGIN
      IF NEW.is_synthetic = true THEN NEW.do_not_contact := true; END IF;
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql
  `);
  await db.query(`
    DROP TRIGGER IF EXISTS prospects_synthetic_suppression ON prospects;
    CREATE TRIGGER prospects_synthetic_suppression
    BEFORE INSERT OR UPDATE OF is_synthetic, do_not_contact ON prospects
    FOR EACH ROW EXECUTE FUNCTION enforce_synthetic_prospect_suppression()
  `);
  await db.query(`
    ALTER TABLE clients
      ADD COLUMN IF NOT EXISTS setter_pipeline_v2_enabled BOOLEAN NOT NULL DEFAULT false,
      ADD COLUMN IF NOT EXISTS setter_pipeline_v2_configured_at TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS setter_review_sample_percent INTEGER NOT NULL DEFAULT 20
  `);
  await db.query(`
    ALTER TABLE call_dispositions
      ADD COLUMN IF NOT EXISTS structured_notes JSONB,
      ADD COLUMN IF NOT EXISTS details JSONB NOT NULL DEFAULT '{}'::jsonb,
      ADD COLUMN IF NOT EXISTS activity_result TEXT,
      ADD COLUMN IF NOT EXISTS next_action TEXT,
      ADD COLUMN IF NOT EXISTS suppression_state TEXT,
      ADD COLUMN IF NOT EXISTS lifecycle_result TEXT,
      ADD COLUMN IF NOT EXISTS is_synthetic BOOLEAN NOT NULL DEFAULT false,
      ADD COLUMN IF NOT EXISTS review_required BOOLEAN NOT NULL DEFAULT false,
      ADD COLUMN IF NOT EXISTS review_status TEXT NOT NULL DEFAULT 'not_sampled',
      ADD COLUMN IF NOT EXISTS reviewed_by INTEGER,
      ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS review_score INTEGER,
      ADD COLUMN IF NOT EXISTS review_notes TEXT,
      ADD COLUMN IF NOT EXISTS review_outcome_accurate BOOLEAN,
      ADD COLUMN IF NOT EXISTS review_notes_complete BOOLEAN,
      ADD COLUMN IF NOT EXISTS idempotency_key TEXT
  `);
  await db.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS call_dispositions_idempotency_idx
      ON call_dispositions (client_id, idempotency_key)
      WHERE idempotency_key IS NOT NULL
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS setter_callbacks (
      id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
      client_id INTEGER NOT NULL,
      prospect_id UUID NOT NULL REFERENCES prospects(id) ON DELETE CASCADE,
      source_disposition_id INTEGER REFERENCES call_dispositions(id),
      due_at TIMESTAMPTZ NOT NULL,
      status TEXT NOT NULL DEFAULT 'pending',
      created_by INTEGER,
      completed_by_disposition_id INTEGER REFERENCES call_dispositions(id),
      completed_at TIMESTAMPTZ,
      cancelled_at TIMESTAMPTZ,
      is_synthetic BOOLEAN NOT NULL DEFAULT false,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      CHECK (status IN ('pending', 'completed', 'cancelled', 'superseded'))
    )
  `);
  await db.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS setter_callbacks_one_pending_idx
      ON setter_callbacks (client_id, prospect_id)
      WHERE status = 'pending'
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS setter_callbacks_due_idx
      ON setter_callbacks (client_id, status, due_at)
  `);
  await db.query(`
    CREATE OR REPLACE FUNCTION cleanup_suppressed_prospect_work()
    RETURNS trigger AS $$
    BEGIN
      IF NEW.do_not_contact = true AND COALESCE(OLD.do_not_contact, false) = false THEN
        UPDATE setter_callbacks
        SET status = 'cancelled', cancelled_at = NOW(), updated_at = NOW()
        WHERE client_id = NEW.client_id AND prospect_id = NEW.id AND status = 'pending';
        IF to_regclass('public.setter_follow_up_drafts') IS NOT NULL THEN
          EXECUTE 'UPDATE setter_follow_up_drafts SET status = ''dismissed'', dismissed_at = NOW(), updated_at = NOW() WHERE client_id = $1 AND prospect_id = $2 AND status IN (''draft'',''reviewed'')'
          USING NEW.client_id, NEW.id;
        END IF;
      END IF;
      RETURN NEW;
    END;
    $$ LANGUAGE plpgsql
  `);
  await db.query(`
    DROP TRIGGER IF EXISTS prospects_suppression_cleanup ON prospects;
    CREATE TRIGGER prospects_suppression_cleanup
    AFTER UPDATE OF do_not_contact, is_synthetic ON prospects
    FOR EACH ROW EXECUTE FUNCTION cleanup_suppressed_prospect_work()
  `);
  await db.query(`
    INSERT INTO setter_callbacks (client_id, prospect_id, due_at, is_synthetic)
    SELECT p.client_id, p.id, p.callback_at, COALESCE(p.is_synthetic, false)
    FROM prospects p
    WHERE p.callback_at IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM setter_callbacks sc
        WHERE sc.client_id = p.client_id AND sc.prospect_id = p.id AND sc.status = 'pending'
      )
    ON CONFLICT DO NOTHING
  `);
}

async function ensureCallDispositionSchema(db) {
  await ensureLegacyCallDispositionSchema(db);
  // Gate 1 / controlled migration: never soft-create Phase 3D from an absent schema.
  // After the approved migration lands, reconcile remaining IF NOT EXISTS objects only.
  if (!(await isPhase3dSetterSchemaPresent(db))) return { phase3d: false };
  await reconcilePhase3dSetterSchema(db);
  return { phase3d: true };
}

function nextBusinessDayTen(now = new Date()) {
  const date = new Date(now);
  date.setDate(date.getDate() + 1);
  date.setHours(10, 0, 0, 0);
  while ([0, 6].includes(date.getDay())) date.setDate(date.getDate() + 1);
  return date;
}

function nurtureCallback(now = new Date()) {
  const date = new Date(now);
  date.setDate(date.getDate() + 90);
  date.setHours(10, 0, 0, 0);
  return date;
}

function resolveCallbackAt(disposition, requestedCallback, now = new Date()) {
  if (requestedCallback) return new Date(requestedCallback);
  if (disposition === 'answered_callback' || disposition === 'gatekeeper_relayed') {
    return nextBusinessDayTen(now);
  }
  if (disposition === 'incumbent_all_set') return nurtureCallback(now);
  return null;
}

async function applyProspectDisposition(db, {
  prospectId,
  clientId,
  disposition,
  callbackAt = null,
}) {
  if (!DISPOSITION_SET.has(disposition)) throw new Error(`Invalid call disposition: ${disposition}`);

  let changes;
  if (['voicemail', 'no_answer'].includes(disposition)) {
    changes = `setter_status = 'contacted', callback_at = $3`;
  } else if (['gatekeeper_relayed', 'answered_callback', 'gatekeeper_blocked'].includes(disposition)) {
    changes = `setter_status = 'follow_up', callback_at = $3`;
  } else if (disposition === 'answered_interested') {
    changes = `status = 'warm', setter_status = 'follow_up', is_hot = true, callback_at = $3`;
  } else if (disposition === 'qualified') {
    changes = `status = 'hot', setter_status = 'follow_up', is_hot = true, callback_at = $3`;
  } else if (disposition === 'incumbent_all_set') {
    changes = `status = 'cold', setter_status = 'follow_up', is_hot = false, callback_at = $3`;
  } else if (disposition === 'answered_not_interested' || disposition === 'disqualified') {
    changes = `status = 'dead', setter_status = 'dead', callback_at = NULL`;
  } else {
    // A wrong or disconnected number closes only the phone path. Other channels
    // remain eligible because global status and DNC are intentionally unchanged.
    changes = `phone = NULL, setter_status = 'dead', callback_at = NULL`;
  }
  const params = changes.includes('$3')
    ? [prospectId, clientId, callbackAt]
    : [prospectId, clientId];

  const result = await db.query(`
    UPDATE prospects
    SET ${changes},
        setter_updated_at = NOW(),
        last_contacted_at = NOW(),
        updated_at = NOW()
    WHERE id = $1 AND client_id = $2
    RETURNING *
  `, params);
  return result.rows[0] || null;
}

module.exports = {
  DISPOSITION_SET,
  DISPOSITION_VALUES,
  applyProspectDisposition,
  ensureCallDispositionSchema,
  ensureLegacyCallDispositionSchema,
  isPhase3dSetterSchemaPresent,
  notSyntheticSql,
  nextBusinessDayTen,
  nurtureCallback,
  resetPhase3dSchemaCache,
  resolveCallbackAt,
};
