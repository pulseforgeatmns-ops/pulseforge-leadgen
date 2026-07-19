BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER TABLE prospects
  ADD COLUMN IF NOT EXISTS is_synthetic BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS synthetic_label TEXT,
  ADD COLUMN IF NOT EXISTS callback_completed_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS assigned_setter_id INTEGER REFERENCES users(id);

UPDATE prospects SET do_not_contact = true WHERE is_synthetic = true;

CREATE OR REPLACE FUNCTION enforce_synthetic_prospect_suppression()
RETURNS trigger AS $$
BEGIN
  IF NEW.is_synthetic = true THEN NEW.do_not_contact := true; END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS prospects_synthetic_suppression ON prospects;
CREATE TRIGGER prospects_synthetic_suppression
BEFORE INSERT OR UPDATE OF is_synthetic, do_not_contact ON prospects
FOR EACH ROW EXECUTE FUNCTION enforce_synthetic_prospect_suppression();

ALTER TABLE clients
  ADD COLUMN IF NOT EXISTS setter_pipeline_v2_enabled BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS setter_pipeline_v2_configured_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS setter_review_sample_percent INTEGER NOT NULL DEFAULT 20;

ALTER TABLE call_dispositions
  ADD COLUMN IF NOT EXISTS structured_notes JSONB,
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
  ADD COLUMN IF NOT EXISTS idempotency_key TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS call_dispositions_idempotency_idx
  ON call_dispositions (client_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS setter_callbacks (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE RESTRICT,
  prospect_id UUID NOT NULL REFERENCES prospects(id) ON DELETE CASCADE,
  source_disposition_id INTEGER REFERENCES call_dispositions(id),
  due_at TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','completed','cancelled','superseded')),
  created_by INTEGER,
  completed_by_disposition_id INTEGER REFERENCES call_dispositions(id),
  completed_at TIMESTAMPTZ,
  cancelled_at TIMESTAMPTZ,
  is_synthetic BOOLEAN NOT NULL DEFAULT false,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS setter_callbacks_one_pending_idx
  ON setter_callbacks (client_id, prospect_id) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS setter_callbacks_due_idx
  ON setter_callbacks (client_id, status, due_at);

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
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS prospects_suppression_cleanup ON prospects;
CREATE TRIGGER prospects_suppression_cleanup
AFTER UPDATE OF do_not_contact, is_synthetic ON prospects
FOR EACH ROW EXECUTE FUNCTION cleanup_suppressed_prospect_work();

INSERT INTO setter_callbacks (client_id, prospect_id, due_at, is_synthetic)
SELECT p.client_id, p.id, p.callback_at, COALESCE(p.is_synthetic, false)
FROM prospects p
WHERE p.callback_at IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM setter_callbacks sc
    WHERE sc.client_id = p.client_id AND sc.prospect_id = p.id AND sc.status = 'pending'
  )
ON CONFLICT DO NOTHING;

COMMIT;
