-- Rollback SPEC-104 Persistent Operator Context

DROP TABLE IF EXISTS operator_context_rebuild_events;
DROP TABLE IF EXISTS operator_contexts;
