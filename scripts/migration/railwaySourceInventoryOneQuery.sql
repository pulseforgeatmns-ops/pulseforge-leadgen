-- Pulseforge/Signal source audit: ONE read-only statement for Railway's PostgreSQL query console.
-- Returns a single JSON object. No customer rows or credentials are selected.
WITH sizes AS (
 SELECT n.nspname AS schema_name,c.relname AS table_name,
        pg_total_relation_size(c.oid) AS total_bytes,
        pg_relation_size(c.oid) AS heap_bytes,
        pg_indexes_size(c.oid) AS index_bytes,
        c.reltuples::bigint AS estimated_rows
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE c.relkind IN ('r','p') AND n.nspname NOT IN ('pg_catalog','information_schema')
), fks AS (
 SELECT sn.nspname AS source_schema,s.relname AS source_table,
        dn.nspname AS target_schema,d.relname AS target_table,
        con.conname AS constraint_name
 FROM pg_constraint con
 JOIN pg_class s ON s.oid=con.conrelid JOIN pg_namespace sn ON sn.oid=s.relnamespace
 JOIN pg_class d ON d.oid=con.confrelid JOIN pg_namespace dn ON dn.oid=d.relnamespace
 WHERE con.contype='f' AND sn.nspname NOT IN ('pg_catalog','information_schema')
)
SELECT jsonb_build_object(
 'database',current_database(),
 'postgres_version',current_setting('server_version'),
 'database_bytes',pg_database_size(current_database()),
 'tables',(SELECT jsonb_agg(to_jsonb(sizes) ORDER BY total_bytes DESC) FROM sizes),
 'foreign_keys',(SELECT coalesce(jsonb_agg(to_jsonb(fks)), '[]'::jsonb) FROM fks)
) AS migration_inventory;
