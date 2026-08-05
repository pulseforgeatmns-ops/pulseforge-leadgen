-- SPEC-064 Relationship Intelligence — idempotent CHECK constraint repair
-- Ensures named enum CHECKs exist even when tables were created earlier via
-- CREATE TABLE IF NOT EXISTS without constraints (or with unnamed/rewritten checks).
-- Rollback: migrations/2026-08-05-relationship-intelligence-constraints.rollback.sql
-- Does NOT touch CRM companies/prospects/opportunities.

DO $ri_constraints$
BEGIN
  IF to_regclass('public.relationship_interactions') IS NULL THEN
    RAISE NOTICE 'relationship_interactions missing — apply 2026-08-04-relationship-intelligence-interview.sql first';
    RETURN;
  END IF;

  -- Drop prior auto-named or repair-named constraints, then reinstall stable names.
  ALTER TABLE relationship_interactions
    DROP CONSTRAINT IF EXISTS relationship_interactions_interaction_type_check;
  ALTER TABLE relationship_interactions
    ADD CONSTRAINT relationship_interactions_interaction_type_check
    CHECK (interaction_type IN (
      'cold_call',
      'discovery_call',
      'walkthrough',
      'estimate',
      'meeting',
      'demo',
      'proposal_review',
      'follow_up',
      'other'
    ));

  ALTER TABLE relationship_interactions
    DROP CONSTRAINT IF EXISTS relationship_interactions_status_check;
  ALTER TABLE relationship_interactions
    ADD CONSTRAINT relationship_interactions_status_check
    CHECK (status IN ('draft', 'reviewed', 'committed'));

  IF to_regclass('public.relationship_interaction_insights') IS NULL THEN
    RAISE NOTICE 'relationship_interaction_insights missing — apply foundation migration first';
    RETURN;
  END IF;

  ALTER TABLE relationship_interaction_insights
    DROP CONSTRAINT IF EXISTS relationship_interaction_insights_kind_check;
  ALTER TABLE relationship_interaction_insights
    ADD CONSTRAINT relationship_interaction_insights_kind_check
    CHECK (kind IN (
      'pain',
      'goal',
      'objection',
      'timeline',
      'budget',
      'decision_maker',
      'stakeholder',
      'competitor',
      'next_step',
      'commitment',
      'risk',
      'buying_signal',
      'open_question',
      'preference',
      'context'
    ));
END
$ri_constraints$;
