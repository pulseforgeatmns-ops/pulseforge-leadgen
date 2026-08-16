-- AO direct mail campaign follow-ups (Campaign 001 warm in-person revisits)

ALTER TABLE ao_leads ADD COLUMN IF NOT EXISTS campaign_name TEXT;

-- Extend task priority to include warm (direct mail pre-contact follow-ups)
ALTER TABLE ao_follow_up_tasks DROP CONSTRAINT IF EXISTS ao_follow_up_tasks_priority_check;
ALTER TABLE ao_follow_up_tasks ADD CONSTRAINT ao_follow_up_tasks_priority_check
  CHECK (priority IN ('normal', 'high', 'warm'));

ALTER TABLE ao_max_sessions DROP CONSTRAINT IF EXISTS ao_max_sessions_mode_check;
ALTER TABLE ao_max_sessions ADD CONSTRAINT ao_max_sessions_mode_check
  CHECK (mode IN ('log_visit', 'follow_up', 'direct_mail_follow_up', 'book_walkthrough', 'daily_debrief', 'ask_for_help'));

CREATE INDEX IF NOT EXISTS idx_ao_leads_campaign ON ao_leads(client_id, campaign_name)
  WHERE campaign_name IS NOT NULL;
