-- Rollback SPEC-014 Knowledge Dual-Write tables only.
DROP TABLE IF EXISTS knowledge_flight_stages;
DROP TABLE IF EXISTS knowledge_sync_ledger;
DROP TABLE IF EXISTS knowledge_outbox;
