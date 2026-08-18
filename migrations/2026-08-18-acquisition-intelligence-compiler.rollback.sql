-- SPEC-113 rollback — drop compiler tables and restore AIM status check.

DROP TABLE IF EXISTS aic_reviews;
DROP TABLE IF EXISTS aic_edges;
DROP TABLE IF EXISTS aic_concepts;
DROP TABLE IF EXISTS aic_documents;
DROP TABLE IF EXISTS aic_workspaces;

ALTER TABLE aim_models DROP CONSTRAINT IF EXISTS aim_models_status_check;
ALTER TABLE aim_models ADD CONSTRAINT aim_models_status_check
  CHECK (status IN ('draft', 'complete', 'superseded'));
