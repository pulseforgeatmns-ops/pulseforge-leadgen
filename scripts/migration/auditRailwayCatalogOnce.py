#!/usr/bin/env python3
"""One-time source PostgreSQL metadata audit. No customer data, writes or secrets."""
from __future__ import annotations
import json
import os
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone

SQL = r"""
WITH objects AS (
 SELECT n.nspname AS schema_name, c.relname AS table_name, c.relkind,
        pg_total_relation_size(c.oid) AS total_bytes, c.reltuples::bigint AS estimated_rows,
        COALESCE((SELECT pn.nspname||'.'||pc.relname FROM pg_inherits h
                  JOIN pg_class pc ON pc.oid=h.inhparent JOIN pg_namespace pn ON pn.oid=pc.relnamespace
                  WHERE h.inhrelid=c.oid LIMIT 1),'') AS parent_table
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname NOT IN ('pg_catalog','information_schema')
   AND n.nspname NOT LIKE 'pg_toast%' AND c.relkind IN ('r','p','v','m','S')
), fks AS (
 SELECT ns.nspname||'.'||s.relname AS source_table,
        nt.nspname||'.'||t.relname AS target_table, con.conname AS constraint_name
 FROM pg_constraint con
 JOIN pg_class s ON s.oid=con.conrelid JOIN pg_namespace ns ON ns.oid=s.relnamespace
 JOIN pg_class t ON t.oid=con.confrelid JOIN pg_namespace nt ON nt.oid=t.relnamespace
 WHERE con.contype='f' AND ns.nspname NOT IN ('pg_catalog','information_schema')
), viewdeps AS (
 SELECT DISTINCT nv.nspname||'.'||v.relname AS view_name,
     nr.nspname||'.'||ref.relname AS referenced_table
 FROM pg_rewrite rw JOIN pg_class v ON v.oid=rw.ev_class
 JOIN pg_namespace nv ON nv.oid=v.relnamespace
 JOIN pg_depend d ON d.classid='pg_rewrite'::regclass AND d.objid=rw.oid
 JOIN pg_class ref ON ref.oid=d.refobjid
 JOIN pg_namespace nr ON nr.oid=ref.relnamespace
 WHERE v.relkind IN ('v','m') AND ref.oid<>v.oid AND
       nv.nspname NOT IN ('pg_catalog','information_schema') AND
       nr.nspname NOT IN ('pg_catalog','information_schema')
), triggers AS (
 SELECT n.nspname||'.'||c.relname AS table_name,
   t.tgname AS trigger_name, pn.nspname||'.'||p.proname AS function_name
 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
 JOIN pg_namespace n ON n.oid=c.relnamespace
 JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_namespace pn ON pn.oid=p.pronamespace
 WHERE NOT t.tgisinternal AND n.nspname NOT IN ('pg_catalog','information_schema')
)
SELECT jsonb_build_object(
 'sourceVersion',current_setting('server_version'),
 'sourceDb',current_database(),
 'databaseBytes',pg_database_size(current_database()),
 'objects',(SELECT COALESCE(jsonb_agg(to_jsonb(objects) ORDER BY total_bytes DESC), '[]'::jsonb) FROM objects),
 'foreignKeys',(SELECT COALESCE(jsonb_agg(to_jsonb(fks)), '[]'::jsonb) FROM fks),
 'viewDependencies',(SELECT COALESCE(jsonb_agg(to_jsonb(viewdeps)), '[]'::jsonb) FROM viewdeps),
 'triggers',(SELECT COALESCE(jsonb_agg(to_jsonb(triggers)), '[]'::jsonb) FROM triggers),
 'extensions',(SELECT COALESCE(jsonb_agg(extname ORDER BY extname), '[]'::jsonb) FROM pg_extension)
);
"""

def owner(name: str) -> str:
    short = name.split(".", 1)[-1]
    if short in {"prospect_signal_events", "warm_signal_state", "warm_signal_events",
                 "warm_trigger_fires"}:
        return "PULSEFORGE"
    if short.startswith("signal_"):
        return "SIGNAL_CANDIDATE"
    return "REVIEW_NON_SIGNAL"

def main():
    if os.getenv("SOURCE_METADATA_AUDIT_APPROVED") != "YES_READ_ONLY_METADATA":
        raise RuntimeError("metadata_audit_not_authorized")
    for key in ("PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"):
        if not os.getenv(key):
            raise RuntimeError("source_connection_missing")
    if not os.environ["PGHOST"].endswith(".railway.internal"):
        raise RuntimeError("not_private_railway_source")
    env = {k: os.environ[k] for k in ("PATH", "HOME", "PGHOST", "PGPORT", "PGDATABASE",
                                      "PGUSER", "PGPASSWORD", "PGSSLMODE") if k in os.environ}
    env["PGOPTIONS"] = "-c default_transaction_read_only=on -c statement_timeout=90000"
    env["PGCONNECT_TIMEOUT"] = "12"
    proc = subprocess.run(["psql", "-X", "-w", "-A", "-t", "-v", "ON_ERROR_STOP=1", "-c", SQL],
                          env=env, capture_output=True, text=True, timeout=120)
    if proc.returncode != 0:
        raise RuntimeError("catalog_query_failed_return_" + str(proc.returncode))
    catalog = json.loads(proc.stdout.strip())
    objects = catalog["objects"]
    counts = Counter(owner(o["table_name"]) for o in objects if o["relkind"] in ("r", "p"))
    size = Counter()
    for o in objects:
        if o["relkind"] in ("r", "p"):
            size[owner(o["table_name"])] += int(o["total_bytes"])
    crossing = []
    for fk in catalog["foreignKeys"]:
        a, b = owner(fk["source_table"]), owner(fk["target_table"])
        if a != b:
            crossing.append({"source":fk["source_table"],"target":fk["target_table"],
                             "constraint":fk["constraint_name"]})
    print("META_SUMMARY " + json.dumps({
        "status":"READ_ONLY_PROVISIONAL_OWNERSHIP_NOT_APPROVED_FOR_RESTORE",
        "capturedAt":datetime.now(timezone.utc).isoformat(),
        "sourceVersion":catalog["sourceVersion"],
        "sourceDb":catalog["sourceDb"],
        "databaseBytes":catalog["databaseBytes"],
        "objects":len(objects),
        "tables":sum(o["relkind"] in ("r","p") for o in objects),
        "foreignKeys":len(catalog["foreignKeys"]),
        "viewDependencies":len(catalog["viewDependencies"]),
        "triggers":len(catalog["triggers"]),
        "extensions":catalog["extensions"],
        "ownerCounts":dict(counts),
        "ownerBytes":dict(size),
        "heuristicCrossBoundaryFKs":crossing,
    },separators=(",",":"),sort_keys=True), flush=True)
    for typ, items in (("OBJECTS",objects),("FKS",catalog["foreignKeys"]),
                       ("VIEWS",catalog["viewDependencies"]),("TRIGGERS",catalog["triggers"])):
        # Keep output in manageable chunks for read-only connector capture.
        for start in range(0,len(items),12):
            sub = items[start:start+12]
            print("META_"+typ+" "+json.dumps({"from":start,"items":sub},
                separators=(",",":"),sort_keys=True),flush=True)
    print("META_AUDIT_COMPLETE",flush=True)
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print("META_AUDIT_FAILED " + (str(e) if isinstance(e,RuntimeError)
                                     else type(e).__name__),file=sys.stderr,flush=True)
        sys.exit(1)
