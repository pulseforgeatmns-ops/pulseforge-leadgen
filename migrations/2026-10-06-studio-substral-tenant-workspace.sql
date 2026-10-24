-- SPEC-SUBSTRAL-PF-001 — bind tenant_workspaces for Studio Substral (canonical semantic authority).
-- Safe to replay: only inserts when no row exists for the studio-substral client.

INSERT INTO tenant_workspaces (
  client_id,
  tenant_key,
  knowledge_namespace,
  mission_namespace,
  prospect_namespace,
  outcome_namespace,
  aim_namespace,
  campaign_namespace,
  memory_namespace,
  origin,
  lifecycle,
  platform_knowledge_isolated
)
SELECT
  c.id,
  c.slug,
  'tenant:' || c.id || ':knowledge',
  'tenant:' || c.id || ':mission',
  'tenant:' || c.id || ':prospect',
  'tenant:' || c.id || ':outcome',
  'tenant:' || c.id || ':aim:' || c.slug,
  'tenant:' || c.id || ':campaign',
  'tenant:' || c.id || ':memory',
  'migration_backfill',
  'provisioned',
  true
FROM clients c
WHERE c.slug = 'studio-substral'
  AND NOT EXISTS (
    SELECT 1 FROM tenant_workspaces tw WHERE tw.client_id = c.id
  );
