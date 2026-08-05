-- Rollback SPEC-064 Relationship Intelligence CHECK constraint repair
-- Leaves tables intact; removes only the named repair constraints.

ALTER TABLE IF EXISTS relationship_interaction_insights
  DROP CONSTRAINT IF EXISTS relationship_interaction_insights_kind_check;

ALTER TABLE IF EXISTS relationship_interactions
  DROP CONSTRAINT IF EXISTS relationship_interactions_interaction_type_check;

ALTER TABLE IF EXISTS relationship_interactions
  DROP CONSTRAINT IF EXISTS relationship_interactions_status_check;
