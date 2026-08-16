-- AO phone follow-up: adds phone_follow_up Max session mode (schema handled in utils/aoFieldSchema.js)

ALTER TABLE ao_max_sessions DROP CONSTRAINT IF EXISTS ao_max_sessions_mode_check;
ALTER TABLE ao_max_sessions ADD CONSTRAINT ao_max_sessions_mode_check
  CHECK (mode IN (
    'log_visit', 'follow_up', 'direct_mail_follow_up', 'route_follow_up',
    'phone_follow_up', 'book_walkthrough', 'daily_debrief', 'ask_for_help'
  ));
