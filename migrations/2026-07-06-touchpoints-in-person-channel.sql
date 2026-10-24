BEGIN;

ALTER TABLE touchpoints
  DROP CONSTRAINT IF EXISTS touchpoints_channel_check;

ALTER TABLE touchpoints
  ADD CONSTRAINT touchpoints_channel_check
  CHECK (channel IN ('email', 'linkedin', 'facebook', 'manual', 'call', 'sms', 'in_person'));

COMMIT;
