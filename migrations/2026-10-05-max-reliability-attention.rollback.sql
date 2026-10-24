-- Rollback SPEC-MAX-RELIABILITY-003

DROP TABLE IF EXISTS max_attention_heartbeats;
DROP TABLE IF EXISTS max_attention_scheduler_runs;
DROP INDEX IF EXISTS idx_max_attention_items_subject;
DROP INDEX IF EXISTS idx_max_attention_items_due;
DROP TABLE IF EXISTS max_attention_items;
