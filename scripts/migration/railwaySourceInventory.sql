-- Read-only source inventory for Pulseforge Railway -> Render migration.
-- Execute against Railway source as a read-only user. Do not run pg_dump/restore here.
BEGIN TRANSACTION READ ONLY;
SELECT current_database() AS database_name, current_setting('server_version') AS server_version, pg_size_pretty(pg_database_size(current_database())) AS database_size;
SELECT n.nspname AS schema_name, c.relname AS table_name, pg_total_relation_size(c.oid) AS total_bytes,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
       pg_relation_size(c.oid) AS heap_bytes, pg_indexes_size(c.oid) AS index_bytes,
       c.reltuples::bigint AS estimated_rows
FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE c.relkind IN ('r','p') AND n.nspname NOT IN ('pg_catalog','information_schema')
ORDER BY total_bytes DESC, schema_name, table_name;
SELECT con.conname AS constraint_name, src_ns.nspname AS source_schema, src.relname AS source_table,
       dst_ns.nspname AS referenced_schema, dst.relname AS referenced_table,
       pg_get_constraintdef(con.oid) AS definition
FROM pg_constraint con JOIN pg_class src ON src.oid=con.conrelid
JOIN pg_namespace src_ns ON src_ns.oid=src.relnamespace
JOIN pg_class dst ON dst.oid=con.confrelid
JOIN pg_namespace dst_ns ON dst_ns.oid=dst.relnamespace
WHERE con.contype='f' AND src_ns.nspname NOT IN ('pg_catalog','information_schema')
ORDER BY source_schema,source_table,constraint_name;
SELECT schemaname, tablename, indexname, indexdef FROM pg_indexes
WHERE schemaname NOT IN ('pg_catalog','information_schema') ORDER BY schemaname,tablename,indexname;
COMMIT;
