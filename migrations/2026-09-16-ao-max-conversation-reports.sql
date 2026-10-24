-- AO Max persistent conversation + conversation bug reports (SPEC AO Max Field Mode)

ALTER TABLE ao_max_sessions DROP CONSTRAINT IF EXISTS ao_max_sessions_mode_check;
ALTER TABLE ao_max_sessions ADD CONSTRAINT ao_max_sessions_mode_check
  CHECK (mode IN (
    'log_visit', 'follow_up', 'direct_mail_follow_up', 'route_follow_up',
    'phone_follow_up', 'book_walkthrough', 'daily_debrief', 'ask_for_help', 'conversation'
  ));

CREATE TABLE IF NOT EXISTS ao_max_conversation_reports (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id),
  ao_owner_id INTEGER NOT NULL REFERENCES users(id),
  session_id UUID NOT NULL REFERENCES ao_max_sessions(id),
  category TEXT NOT NULL DEFAULT 'user_report',
  note TEXT,
  transcript_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  context_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'new'
    CHECK (status IN ('new', 'reviewed', 'resolved')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ao_max_reports_client_created
  ON ao_max_conversation_reports(client_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ao_max_reports_session
  ON ao_max_conversation_reports(session_id);
