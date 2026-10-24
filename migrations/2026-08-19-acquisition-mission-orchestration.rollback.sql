-- Rollback SPEC-118 Acquisition Mission Orchestration

DROP TABLE IF EXISTS acquisition_mission_learning;
DROP TABLE IF EXISTS acquisition_mission_outcomes;
DROP TABLE IF EXISTS acquisition_mission_observations;
DROP TABLE IF EXISTS acquisition_mission_contributions;
DROP TABLE IF EXISTS acquisition_mission_events;
DROP TABLE IF EXISTS acquisition_missions;
