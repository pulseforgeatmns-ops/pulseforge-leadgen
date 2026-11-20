"""Shared, read-only fingerprint queries (identical on Railway and Render).

Same algorithm as scripts/migration/tableFingerprints.js: count(*) plus an
order-independent md5 multiset digest of md5(row::text). Emits no row data.
"""
from __future__ import annotations

import re

IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
SIGNAL_PREFIX = "signal_"


def is_signal(table: str) -> bool:
    return table.startswith(SIGNAL_PREFIX)


def list_tables(cur) -> list[str]:
    cur.execute("""SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                   WHERE n.nspname='public' AND c.relkind IN ('r','p') ORDER BY 1""")
    names = [r[0] for r in cur.fetchall()]
    bad = [n for n in names if not IDENT_RE.fullmatch(n)]
    if bad:
        raise RuntimeError("unexpected_table_identifier")
    return names


def catalog_counts(cur) -> dict[str, int]:
    q = {
        "tables": "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('r','p')",
        "views": "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('v','m')",
        "sequences": "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind='S'",
        "indexes": "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind IN ('i','I')",
        "triggers": "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND NOT t.tgisinternal",
        "functions": "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public'",
        "constraints": "SELECT count(*) FROM pg_constraint k JOIN pg_namespace n ON n.oid=k.connamespace WHERE n.nspname='public'",
    }
    out = {}
    for name, sql in q.items():
        cur.execute(sql)
        out[name] = int(cur.fetchone()[0])
    return out


def fingerprint_tables(cur, tables: list[str]) -> dict[str, dict]:
    """Sort-free, order-independent multiset digest: the sums of the four
    32-bit words of md5(row::text) over all rows (numeric, no overflow).
    A plain aggregate needs no sort and no temp files, so it is safe on a
    nearly full source volume. Any added, removed or changed row changes it."""
    report = {}
    for name in tables:
        if not IDENT_RE.fullmatch(name):
            raise RuntimeError("unexpected_table_identifier")
        cur.execute(
            "SELECT count(*)::bigint, "
            "coalesce(sum(('x'||substr(h,1,8))::bit(32)::bigint::numeric),0)::text||'.'||"
            "coalesce(sum(('x'||substr(h,9,8))::bit(32)::bigint::numeric),0)::text||'.'||"
            "coalesce(sum(('x'||substr(h,17,8))::bit(32)::bigint::numeric),0)::text||'.'||"
            "coalesce(sum(('x'||substr(h,25,8))::bit(32)::bigint::numeric),0)::text "
            f'FROM (SELECT md5(t::text) AS h FROM public."{name}" t) s'
        )
        rows, digest = cur.fetchone()
        report[name] = {"rows": int(rows), "fingerprint": digest}
    return report


def session_guards(cur) -> None:
    """No parallel workers, and never let a query write temp files."""
    cur.execute("SET LOCAL max_parallel_workers_per_gather = 0")
    cur.execute("SAVEPOINT guard")
    try:
        cur.execute("SET LOCAL temp_file_limit = '8MB'")
        cur.execute("RELEASE SAVEPOINT guard")
    except Exception:  # noqa: BLE001 - non-superuser: plain aggregate still spills nothing
        cur.execute("ROLLBACK TO SAVEPOINT guard")
        cur.execute("RELEASE SAVEPOINT guard")


def count_tables(cur, tables: list[str]) -> dict[str, int]:
    out = {}
    for name in tables:
        cur.execute(f'SELECT count(*)::bigint FROM public."{name}"')
        out[name] = int(cur.fetchone()[0])
    return out


def sequences(cur) -> dict[str, str | None]:
    cur.execute("""SELECT schemaname||'.'||sequencename, last_value::text FROM pg_sequences
                   WHERE schemaname NOT IN ('pg_catalog','information_schema') ORDER BY 1""")
    return {name: value for name, value in cur.fetchall()}


def compare(source: dict, dest: dict) -> list[dict]:
    """Differences between a source manifest and a destination report."""
    diffs: list[dict] = []
    src_t, dst_t = source["fingerprints"], dest["fingerprints"]
    for name in sorted(set(src_t) | set(dst_t)):
        a, b = src_t.get(name), dst_t.get(name)
        if a is None or b is None:
            diffs.append({"kind": "table_not_on_both", "table": name})
        elif a["rows"] != b["rows"]:
            diffs.append({"kind": "row_count", "table": name, "source": a["rows"], "destination": b["rows"]})
        elif a["fingerprint"] != b["fingerprint"]:
            diffs.append({"kind": "content", "table": name, "rows": a["rows"]})
    for name, a in sorted(source["sequences"].items()):
        if name.startswith("public." + SIGNAL_PREFIX):
            continue  # Signal sequence values are excluded with Signal data.
        b = dest["sequences"].get(name, "missing")
        if b == "missing":
            diffs.append({"kind": "sequence_missing", "sequence": name})
        elif a is not None and (b is None or int(b) < int(a)):
            diffs.append({"kind": "sequence_behind", "sequence": name})
    for name in sorted(source.get("signalTables", [])):
        if dest.get("signalRowCounts", {}).get(name, -1) != 0:
            diffs.append({"kind": "signal_table_not_empty_schema", "table": name})
    for key, value in source["catalog"].items():
        if dest["catalog"].get(key) != value:
            diffs.append({"kind": "catalog_count", "object": key,
                          "source": value, "destination": dest["catalog"].get(key)})
    return diffs
