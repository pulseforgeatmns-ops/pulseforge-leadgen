-- Rollback SPEC-117 Emmett Outbound Infrastructure Intelligence

DROP TABLE IF EXISTS emmett_outbound_learning;
DROP TABLE IF EXISTS emmett_outbound_outcomes;
DROP TABLE IF EXISTS emmett_governor_acks;
DROP TABLE IF EXISTS emmett_send_plans;
DROP TABLE IF EXISTS emmett_inbox_snapshots;
