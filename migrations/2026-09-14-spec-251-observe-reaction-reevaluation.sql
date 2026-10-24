-- SPEC-251 — Observe reaction cadence re-evaluation supersession (append-only initial + additive re-eval).

ALTER TABLE acquisition_mission_observe_reactions
  ADD COLUMN IF NOT EXISTS evaluation_kind TEXT NOT NULL DEFAULT 'initial',
  ADD COLUMN IF NOT EXISTS evaluation_sequence INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS reevaluation_trigger_kind TEXT,
  ADD COLUMN IF NOT EXISTS reevaluation_trigger_id TEXT,
  ADD COLUMN IF NOT EXISTS supersedes_reaction_id TEXT;

ALTER TABLE acquisition_mission_observe_reactions
  DROP CONSTRAINT IF EXISTS acquisition_mission_observe_reactions_observation_id_key;

CREATE UNIQUE INDEX IF NOT EXISTS acquisition_mission_observe_reactions_initial_uidx
  ON acquisition_mission_observe_reactions (observation_id)
  WHERE evaluation_kind = 'initial';

CREATE UNIQUE INDEX IF NOT EXISTS acquisition_mission_observe_reactions_reeval_uidx
  ON acquisition_mission_observe_reactions (observation_id, reevaluation_trigger_id)
  WHERE reevaluation_trigger_id IS NOT NULL;
