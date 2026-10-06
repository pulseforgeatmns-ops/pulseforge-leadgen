-- MAX-VOICE-001: Durable voice note provenance (audio stored separately from CRM)

BEGIN;

SELECT pg_advisory_xact_lock(20261006, 1);

CREATE TABLE IF NOT EXISTS max_voice_recordings (
  id TEXT PRIMARY KEY,
  client_id INTEGER NOT NULL REFERENCES clients(id),
  conversation_id TEXT,
  actor_id INTEGER REFERENCES users(id),
  mime_type TEXT NOT NULL,
  duration_ms INTEGER,
  storage_ref TEXT NOT NULL,
  audio_bytea BYTEA,
  transcription_status TEXT NOT NULL DEFAULT 'pending' CHECK (transcription_status IN (
    'pending', 'processing', 'completed', 'failed', 'empty'
  )),
  transcript_text TEXT,
  transcript_segments JSONB NOT NULL DEFAULT '[]'::jsonb,
  transcription_confidence NUMERIC(5, 4),
  transcription_provider TEXT,
  transcription_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  envelope_id TEXT,
  attachment_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_max_voice_recordings_client_created
  ON max_voice_recordings (client_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_max_voice_recordings_conversation
  ON max_voice_recordings (client_id, conversation_id);

-- Allow voice artifacts on ingestion evidence chain
ALTER TABLE max_evidence_artifacts DROP CONSTRAINT IF EXISTS max_evidence_artifacts_artifact_type_check;
ALTER TABLE max_evidence_artifacts ADD CONSTRAINT max_evidence_artifacts_artifact_type_check
  CHECK (artifact_type IN ('message', 'spreadsheet', 'structured_file', 'api_payload', 'voice', 'attachment'));

COMMIT;
