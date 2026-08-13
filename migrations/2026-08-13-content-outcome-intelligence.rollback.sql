-- Rollback SPEC-092 Content Outcome Intelligence
-- Safe only when no dependent production data must be retained.

DROP TABLE IF EXISTS content_qualitative_signals;
DROP TABLE IF EXISTS content_business_outcomes;
DROP TABLE IF EXISTS content_performance_snapshots;
DROP TABLE IF EXISTS content_publications;
DROP TABLE IF EXISTS content_artifacts;
