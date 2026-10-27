-- PROPOSED ONLY. Review existing live indexes and the query plan before apply.
-- Run separately from transaction-wrapped migrations (CONCURRENTLY requires it).
-- This supports the tenant-10 sibling hold read; it changes no review flags.
CREATE INDEX CONCURRENTLY IF NOT EXISTS prospects_anchor_company_review_lookup_idx
  ON prospects (company_id, id) WHERE client_id = 10;
