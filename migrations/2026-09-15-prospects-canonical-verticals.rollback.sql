-- Rollback for migrations/2026-09-15-prospects-canonical-verticals.sql

ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_vertical_canonical_chk;
