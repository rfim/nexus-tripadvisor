"""One contract, two warehouses: the SQL for Snowflake and for BigQuery is generated from contracts/sources.yml,
never written by hand per warehouse. That is the governance tooling: personal data is masked in both because the
contract says it is personal, not because someone remembered.

render() writes warehouse/snowflake/*.sql and warehouse/bigquery/*.sql (committed; CI fails if they drift).
load() runs the same MERGE semantics on SQLite, standing in for both, and checks quality and erasure there.
"""
import json
import sqlite3

from nexus.contracts import load_quality, topic, topics
from nexus.paths import ROOT

OUT = ROOT / "warehouse"

TYPES = {
    "snowflake": {"string": "VARCHAR", "timestamp": "TIMESTAMP_NTZ", "int": "NUMBER(38,0)", "decimal": "NUMBER(12,4)",
                  "bool": "BOOLEAN", "object": "VARIANT"},
    "bigquery": {"string": "STRING", "timestamp": "TIMESTAMP", "int": "INT64", "decimal": "NUMERIC(12,4)",
                 "bool": "BOOL", "object": "JSON"},
    "sqlite": {"string": "TEXT", "timestamp": "TEXT", "int": "INTEGER", "decimal": "REAL", "bool": "INTEGER", "object": "TEXT"},
}

# Gold products: what the stream and batch publish. Raw tables come from the topic schemas.
PRODUCTS = {
    "location_views_hourly": {"cols": {"window_start": "timestamp", "location_id": "string", "views": "int"},
                              "key": ["window_start", "location_id"], "cluster": ["location_id"], "partition": "window_start"},
    "partner_revenue_15m": {"cols": {"window_start": "timestamp", "partner": "string", "clicks": "int", "revenue_gbp": "decimal"},
                            "key": ["window_start", "partner"], "cluster": ["partner"], "partition": "window_start"},
    "reviews_current": {"cols": {"review_id": "string", "location_id": "string", "rating": "int", "status": "string",
                                 "lang": "string", "version": "int"},
                        "key": ["review_id"], "cluster": ["location_id"], "version": "version", "tombstone": "erased"},
}


def raw_columns(t: dict) -> dict:
    """The raw table: every field of every schema version, the v2 price flattened, plus where it came from in Kafka."""
    cols = {}
    for fields in t["schemas"].values():
        for f, ty in fields.items():
            if isinstance(ty, dict):
                cols.update({f"{f}_{k}": v for k, v in ty.items()})
            else:
                cols.setdefault(f, ty)
    if t["topic"] == "partner_clicks":
        cols["revenue_gbp"] = "decimal"
    return cols | {"_schema_version": "int", "_partition": "int", "_offset": "int", "_appended_at": "timestamp"}


def _name(d: str, layer: str, table: str) -> str:
    return f"NEXUS.{layer.upper()}.{table.upper()}" if d == "snowflake" else f"`nexus-data.{layer}.{table}`"


def _ddl(d: str, layer: str, table: str, cols: dict, pii=(), cluster=(), partition=None) -> str:
    lines = []
    for c, ty in cols.items():
        col = f"  {c} {TYPES[d][ty]}"
        if c in pii and d == "bigquery":
            col += " OPTIONS(description = 'personal data: policy tag pii.restricted')"
        lines.append(col)
    sql = f"CREATE TABLE IF NOT EXISTS {_name(d, layer, table)} (\n" + ",\n".join(lines) + "\n)"
    if d == "snowflake" and cluster:
        sql += f"\nCLUSTER BY (TO_DATE({partition}), {', '.join(cluster)})" if partition else f"\nCLUSTER BY ({', '.join(cluster)})"
    if d == "bigquery":
        if partition:
            sql += f"\nPARTITION BY DATE({partition})"
        if cluster:
            sql += f"\nCLUSTER BY {', '.join(cluster)}"
    return sql + ";\n"


def _merge(d: str, table: str, spec: dict) -> str:
    cols, key = list(spec["cols"]), spec["key"]
    tgt, src = _name(d, "gold", table), _name(d, "stage", table)
    now = "CURRENT_TIMESTAMP()"
    on = " AND ".join(f"t.{k} = s.{k}" for k in key)
    vals = [c for c in cols if c not in key]
    changed = " OR ".join(f"t.{c} IS DISTINCT FROM s.{c}" for c in vals)
    sets = ", ".join(f"{c} = s.{c}" for c in vals)
    ins = ", ".join(cols)
    sql = f"MERGE INTO {tgt} t\nUSING {src} s\n  ON {on}\n"
    if "version" in spec:
        v, tomb = spec["version"], spec["tombstone"]
        sql += (f"WHEN MATCHED AND s.{tomb} AND s.{v} > t.{v} THEN DELETE\n"
                f"WHEN MATCHED AND NOT s.{tomb} AND s.{v} > t.{v} THEN UPDATE SET {sets}, _loaded_at = {now}\n"
                f"WHEN NOT MATCHED AND NOT s.{tomb} THEN INSERT ({ins}, _loaded_at)\n  VALUES ({', '.join('s.' + c for c in cols)}, {now});\n")
    else:
        sql += (f"WHEN MATCHED AND ({changed}) THEN UPDATE SET {sets}, _loaded_at = {now}\n"
                f"WHEN NOT MATCHED THEN INSERT ({ins}, _loaded_at)\n  VALUES ({', '.join('s.' + c for c in cols)}, {now});\n")
    return sql


def _governance(d: str) -> str:
    out = ["-- Generated from contracts/sources.yml: every field listed under pii, in every raw table.\n"]
    if d == "snowflake":
        out.append("CREATE MASKING POLICY IF NOT EXISTS NEXUS.GOVERNANCE.PII_STRING AS (v VARCHAR) RETURNS VARCHAR ->\n"
                   "  CASE WHEN IS_ROLE_IN_SESSION('PII_READER') THEN v ELSE SHA2(v, 256) END;\n")
        for t in topics():
            for c in t["pii"]:
                out.append(f"ALTER TABLE {_name(d, 'raw', t['topic'])} MODIFY COLUMN {c} "
                           f"SET MASKING POLICY NEXUS.GOVERNANCE.PII_STRING;\n")
    else:
        out.append("-- BigQuery column-level security: attach the policy tag in the table schema (bq update --schema),\n"
                   "-- and a data masking rule (SHA256) on the tag for every role without Fine-Grained Reader.\n")
        for t in topics():
            for c in t["pii"]:
                out.append(f"-- {_name(d, 'raw', t['topic'])}.{c} → projects/nexus-data/locations/eu/taxonomies/pii/policyTags/restricted\n")
    return "".join(out)


def _erasure(d: str, ids_param: str) -> str:
    """Right to erasure: a review's tombstone in the stream deletes it everywhere it was copied."""
    p = ":review_ids" if d == "snowflake" else "@review_ids"
    in_ = f"IN (SELECT value FROM TABLE(FLATTEN(INPUT => PARSE_JSON({p}))))" if d == "snowflake" else f"IN UNNEST({p})"
    sql = (f"DELETE FROM {_name(d, 'raw', 'reviews')} WHERE review_id {in_};\n"
           f"DELETE FROM {_name(d, 'gold', 'reviews_current')} WHERE review_id {in_};\n")
    if d == "snowflake":
        sql += "-- Time Travel keeps deleted rows for DATA_RETENTION_TIME_IN_DAYS: 1 on raw.reviews, so erasure completes in a day.\n"
    else:
        sql += "-- Time travel keeps deleted rows for the dataset's max_time_travel_hours: 48 on raw, so erasure completes in two days.\n"
    return sql


def render() -> dict:
    files = {}
    for d in ("snowflake", "bigquery"):
        for t in topics():
            files[f"{d}/raw_{t['topic']}.sql"] = _ddl(d, "raw", t["topic"], raw_columns(t), pii=t["pii"],
                                                       cluster=["_partition"], partition="_appended_at")
        for name, spec in PRODUCTS.items():
            cols = spec["cols"] | {"_loaded_at": "timestamp"}
            files[f"{d}/{name}.sql"] = (_ddl(d, "gold", name, cols, cluster=spec["cluster"], partition=spec.get("partition"))
                                        + "\n" + _merge(d, name, spec))
        files[f"{d}/governance.sql"] = _governance(d)
        files[f"{d}/erasure.sql"] = _erasure(d, "review_ids")
    for path, sql in files.items():
        (OUT / path).parent.mkdir(parents=True, exist_ok=True)
        (OUT / path).write_text(sql)
    return files


def load(db_path, gold: dict, raw_reviews: list[dict], locations: list[dict], erased: list[str]) -> dict:
    """Load the gold rows and the raw reviews into SQLite with upsert semantics, then run the checks."""
    db = sqlite3.connect(db_path)
    db.execute("DROP TABLE IF EXISTS dim_location")
    db.execute("CREATE TABLE dim_location (location_id TEXT PRIMARY KEY, city TEXT, kind TEXT, name TEXT)")
    db.executemany("INSERT INTO dim_location VALUES (?,?,?,?)", [(x["location_id"], x["city"], x["kind"], x["name"]) for x in locations])
    for name, spec in PRODUCTS.items():
        cols = list(spec["cols"])
        db.execute(f"DROP TABLE IF EXISTS {name}")
        typed = ", ".join(f"{c} {TYPES['sqlite'][t]}" for c, t in spec["cols"].items())
        db.execute(f"CREATE TABLE {name} ({typed}, PRIMARY KEY ({', '.join(spec['key'])}))")
        upd = ", ".join(f"{c} = excluded.{c}" for c in cols if c not in spec["key"])
        db.executemany(f"INSERT INTO {name} ({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))}) "
                       f"ON CONFLICT ({', '.join(spec['key'])}) DO UPDATE SET {upd}",
                       [tuple(r[c] for c in cols) for r in gold[name]])
    rtypes = raw_columns(topic("reviews"))
    rcols = list(rtypes)
    db.execute("DROP TABLE IF EXISTS raw_reviews")
    db.execute(f"CREATE TABLE raw_reviews ({', '.join(c + ' ' + TYPES['sqlite'][t] for c, t in rtypes.items())})")
    db.executemany(f"INSERT INTO raw_reviews VALUES ({', '.join('?' * len(rcols))})", [tuple(r.get(c) for c in rcols) for r in raw_reviews])
    queries = []

    def q(sql, args=()):
        """Run a query and keep it with its result: the modeling page shows them as a sqlite3 session."""
        cur = db.execute(sql, args)
        rows = [list(r) for r in cur.fetchall()]
        queries.append({"sql": sql if not args else sql.replace("?", "{}").format(*(repr(a) for a in args)),
                        "cols": [c[0] for c in cur.description] if cur.description else [], "rows": rows})
        return rows

    q("SELECT name AS column_name, type FROM pragma_table_info('raw_reviews') WHERE name LIKE 'reviewer%' OR name IN ('review_id','version','op')")
    q("SELECT location_id, window_start, views FROM location_views_hourly ORDER BY views DESC, window_start LIMIT 3")
    q("SELECT partner, count(*) AS windows, sum(clicks) AS clicks, round(sum(revenue_gbp), 2) AS revenue_gbp "
      "FROM partner_revenue_15m GROUP BY partner ORDER BY revenue_gbp DESC")
    q("SELECT l.city, count(*) AS reviews, round(avg(r.rating), 2) AS avg_rating FROM reviews_current r "
      "JOIN dim_location l USING (location_id) WHERE r.status = 'published' GROUP BY l.city ORDER BY reviews DESC LIMIT 4")
    q(f"SELECT review_id, version, op, reviewer_hash FROM raw_reviews WHERE review_id IN ({', '.join('?' * len(erased))}) ORDER BY version", erased)
    before = db.execute(f"SELECT count(*) FROM raw_reviews WHERE review_id IN ({', '.join('?' * len(erased))})", erased).fetchone()[0]
    db.execute(f"DELETE FROM raw_reviews WHERE review_id IN ({', '.join('?' * len(erased))})", erased)
    db.execute(f"DELETE FROM reviews_current WHERE review_id IN ({', '.join('?' * len(erased))})", erased)
    db.commit()
    q(f"SELECT count(*) AS rows_left FROM raw_reviews WHERE review_id IN ({', '.join('?' * len(erased))})", erased)
    q("SELECT count(*) AS failing FROM partner_revenue_15m WHERE NOT (revenue_gbp / clicks BETWEEN 0.10 AND 2.00)")
    checks = []
    for table, rules in load_quality()["rules"].items():
        n = db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        for rule, cond in rules.items():
            bad = db.execute(f"SELECT count(*) FROM {table} WHERE NOT ({cond})").fetchone()[0]
            checks.append({"table": table, "check": rule, "rule": cond, "rows": n, "failing": bad,
                           "status": "pass" if bad == 0 else "fail"})
    left = sum(db.execute(f"SELECT count(*) FROM {t} WHERE review_id IN ({', '.join('?' * len(erased))})", erased).fetchone()[0]
               for t in ("raw_reviews", "reviews_current"))
    checks.append({"table": "raw_reviews, reviews_current", "check": "erased_reviews_gone", "rule": "review_id NOT IN (erased)",
                   "rows": before, "failing": left, "status": "pass" if left == 0 else "fail"})
    rendered = {p: (OUT / p).read_text() for p in ("snowflake/governance.sql", "bigquery/governance.sql")}
    for t in topics():
        for c in t["pii"]:
            ok = f"{t['topic'].upper()} MODIFY COLUMN {c} SET MASKING" in rendered["snowflake/governance.sql"] and \
                 f"{t['topic']}`.{c} →" in rendered["bigquery/governance.sql"]
            checks.append({"table": f"raw_{t['topic']}", "check": f"pii_masked:{c}", "rule": "masking policy (Snowflake) and policy tag (BigQuery)",
                           "rows": None, "failing": 0 if ok else 1, "status": "pass" if ok else "fail"})
    counts = {name: db.execute(f"SELECT count(*) FROM {name}").fetchone()[0] for name in PRODUCTS}
    db.close()
    return {"checks": checks, "rows": counts, "erased_rows_removed": before, "queries": queries}


def model() -> dict:
    """The model as data, for the modeling page: raw tables per topic (with personal fields), the Gold products with
    their grain, keys and layout, and the lineage edges between them."""
    raw = [{"table": f"raw_{t['topic']}", "topic": t["topic"], "owner": t["owner"],
            "columns": [{"name": c, "type": ty, "pii": c in t["pii"]} for c, ty in raw_columns(t).items()]} for t in topics()]
    src = {"location_views_hourly": "page_views", "partner_revenue_15m": "partner_clicks", "reviews_current": "reviews"}
    grain = {"location_views_hourly": "one row per location per hour", "partner_revenue_15m": "one row per partner per 15 minutes",
             "reviews_current": "one row per live review (latest version)"}
    gold = [{"table": n, "grain": grain[n], "key": sp["key"], "cluster": sp["cluster"], "partition": sp.get("partition"),
             "columns": [{"name": c, "type": ty} for c, ty in sp["cols"].items()], "from": f"raw_{src[n]}",
             "sla_minutes": topic(src[n])["product"]["sla"]["freshness_minutes"], "tier": topic(src[n])["product"]["sla"]["tier"]}
            for n, sp in PRODUCTS.items()]
    edges = [[f"topic:{t['topic']}", f"raw_{t['topic']}"] for t in topics()] + [[g["from"], g["table"]] for g in gold] + \
            [["dim_location", "location_views_hourly"], ["dim_location", "reviews_current"]]
    return {"raw": raw, "gold": gold, "edges": edges}


def dialect_diff() -> list[dict]:
    """Where the two warehouses differ for the same contract, for the site."""
    return [{"concern": "Table name", "snowflake": _name("snowflake", "gold", "partner_revenue_15m"),
             "bigquery": _name("bigquery", "gold", "partner_revenue_15m")},
            {"concern": "Money", "snowflake": TYPES["snowflake"]["decimal"], "bigquery": TYPES["bigquery"]["decimal"]},
            {"concern": "Event time", "snowflake": TYPES["snowflake"]["timestamp"], "bigquery": TYPES["bigquery"]["timestamp"]},
            {"concern": "Layout", "snowflake": "CLUSTER BY (TO_DATE(window_start), partner)",
             "bigquery": "PARTITION BY DATE(window_start) CLUSTER BY partner"},
            {"concern": "Personal data", "snowflake": "masking policy, SHA2 unless PII_READER",
             "bigquery": "policy tag + data masking rule"},
            {"concern": "Erasure list", "snowflake": "FLATTEN(PARSE_JSON(:review_ids))", "bigquery": "UNNEST(@review_ids)"},
            {"concern": "Deleted rows linger", "snowflake": "Time Travel, 1 day on raw", "bigquery": "time travel, 48 hours on raw"}]


if __name__ == "__main__":
    print(json.dumps(sorted(render()), indent=1))
