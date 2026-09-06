-- SPEC-245A. Run manually ONLY AFTER SPEC-245B is deployed.
-- One transaction; no migration bookkeeping or other table writes.
BEGIN;
SET LOCAL lock_timeout = '10s';
LOCK TABLE cie_evidence IN ACCESS EXCLUSIVE MODE;

-- A timestamp-only historical row is protected by this trigger. Suspend
-- only this trigger under the lock; rollback restores it on any failure.
ALTER TABLE cie_evidence DISABLE TRIGGER canonical_cie_evidence_immutable_trigger;
UPDATE cie_evidence
SET source_text_sha256 = COALESCE(source_text_sha256, encode(digest(statement,'sha256'),'hex')),
    immutable_at = COALESCE(immutable_at, created_at)
WHERE source_text_sha256 IS NULL OR immutable_at IS NULL;
ALTER TABLE cie_evidence ENABLE TRIGGER canonical_cie_evidence_immutable_trigger;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM cie_evidence
    WHERE source_text_sha256 IS NULL OR immutable_at IS NULL
       OR source_text_sha256 <> encode(digest(statement,'sha256'),'hex')) THEN
    RAISE EXCEPTION 'SPEC-245A validation failed: missing or invalid immutable metadata';
  END IF;
END $$;
COMMIT;
