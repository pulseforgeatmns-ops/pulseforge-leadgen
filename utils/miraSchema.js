const pool = require('../db');

async function ensureMiraSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS capture_inbox (
      id              BIGSERIAL PRIMARY KEY,
      received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      telegram_msg_id BIGINT,
      content_type    TEXT NOT NULL CHECK (content_type IN ('text', 'voice', 'photo', 'document', 'link')),
      raw_text        TEXT,
      voice_file_id   TEXT,
      voice_url       TEXT,
      transcript      TEXT,
      photo_file_id   TEXT,
      photo_url       TEXT,
      link_url        TEXT,
      classification  TEXT,
      confidence      NUMERIC(3,2),
      client_id       INT,
      routed_to_table TEXT,
      routed_to_id    TEXT,
      status          TEXT NOT NULL DEFAULT 'new'
                      CHECK (status IN ('new', 'transcribed', 'classified', 'routed', 'review_needed', 'failed')),
      raw_metadata    JSONB,
      classifier_notes TEXT,
      processed_at    TIMESTAMPTZ
    )
  `);

  await pool.query(`
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'capture_inbox'
          AND column_name = 'routed_to_id'
          AND data_type <> 'text'
      ) THEN
        ALTER TABLE capture_inbox
        ALTER COLUMN routed_to_id TYPE TEXT USING routed_to_id::TEXT;
      END IF;
    END $$;
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_capture_inbox_status ON capture_inbox(status)
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_capture_inbox_received_at ON capture_inbox(received_at DESC)
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS client_notes (
      id             BIGSERIAL PRIMARY KEY,
      client_id      INT,
      capture_id     BIGINT REFERENCES capture_inbox(id),
      content        TEXT NOT NULL,
      note_type      TEXT,
      created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      source         TEXT NOT NULL DEFAULT 'mira'
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_client_notes_client_id ON client_notes(client_id)
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_client_notes_created_at ON client_notes(created_at DESC)
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS ideas (
      id          BIGSERIAL PRIMARY KEY,
      capture_id  BIGINT REFERENCES capture_inbox(id),
      content     TEXT NOT NULL,
      tags        TEXT[],
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      archived    BOOLEAN NOT NULL DEFAULT FALSE
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS content_seeds (
      id          BIGSERIAL PRIMARY KEY,
      capture_id  BIGINT REFERENCES capture_inbox(id),
      content     TEXT NOT NULL,
      brand       TEXT,
      channel     TEXT,
      used        BOOLEAN NOT NULL DEFAULT FALSE,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS blockers (
      id          BIGSERIAL PRIMARY KEY,
      capture_id  BIGINT REFERENCES capture_inbox(id),
      client_id   INT,
      content     TEXT NOT NULL,
      blocking    TEXT,
      resolved    BOOLEAN NOT NULL DEFAULT FALSE,
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      resolved_at TIMESTAMPTZ
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS refs (
      id          BIGSERIAL PRIMARY KEY,
      capture_id  BIGINT REFERENCES capture_inbox(id),
      content     TEXT NOT NULL,
      ref_type    TEXT,
      tags        TEXT[],
      created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS reminders (
      id           BIGSERIAL PRIMARY KEY,
      capture_id   BIGINT REFERENCES capture_inbox(id),
      content      TEXT NOT NULL,
      remind_at    TIMESTAMPTZ NOT NULL,
      sent         BOOLEAN NOT NULL DEFAULT FALSE,
      created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS mira_corrections (
      id                  BIGSERIAL PRIMARY KEY,
      capture_id          BIGINT REFERENCES capture_inbox(id),
      original_class      TEXT NOT NULL,
      corrected_class     TEXT NOT NULL,
      original_routed_to  TEXT,
      corrected_routed_to TEXT,
      note                TEXT,
      created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
}

module.exports = { ensureMiraSchema };
