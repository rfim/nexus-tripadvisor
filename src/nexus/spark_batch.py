"""The nightly batch in PySpark: the same answer as batch.recompute, at warehouse scale.

Reads the topic archive (one JSON line per Kafka record), parses each value against the union of its schema versions,
counts what can't be parsed, keeps the first arrival of each id, and windows by event time.
tests/test_spark_batch.py checks it matches the plain-Python batch window for window.
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from nexus.contracts import fx, topic
from nexus.paths import LANDING

VALUE = T.StructType([T.StructField(f, T.StringType()) for f in
                      ("event_id", "click_id", "event_time", "location_id", "partner", "user_hash")] +
                     [T.StructField("cpc_gbp", T.DoubleType()),
                      T.StructField("price", T.StructType([T.StructField("amount", T.DoubleType()),
                                                           T.StructField("currency", T.StringType())]))])


def get_spark(app: str = "nexus-batch") -> SparkSession:
    return (SparkSession.builder.appName(app).master("local[2]")
            .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false").getOrCreate())


def recompute(spark: SparkSession, name: str) -> dict:
    t = topic(name)
    prod = t["product"]
    size = prod["window_minutes"] * 60_000
    raw = (spark.read.json(str(LANDING / "kafka" / name / "p*.jsonl"))
           .withColumn("partition", F.regexp_extract(F.input_file_name(), r"p(\d+)\.jsonl", 1).cast("int"))
           .withColumn("v", F.from_json("value", VALUE)))
    rates = F.create_map(*[x for k, r in fx().items() for x in (F.lit(k), F.lit(r))])
    parsed = (raw.withColumn("ok", F.col("v.event_time").isNotNull())
              .withColumn("id", F.col(f"v.{t['id']}"))
              .withColumn("et", F.unix_millis(F.to_timestamp("v.event_time", "yyyy-MM-dd'T'HH:mm:ss.SSSX")))
              .withColumn("revenue_gbp", F.coalesce(F.col("v.cpc_gbp"),
                                                    F.round(F.col("v.price.amount") * rates[F.col("v.price.currency")], 4))))
    bad = parsed.filter(~F.col("ok")).count()
    first = Window.partitionBy("id").orderBy("ts", "partition", "offset")
    events = (parsed.filter("ok").withColumn("rn", F.row_number().over(first)).filter("rn = 1")
              .withColumn("window_start", F.col("et") - F.pmod(F.col("et"), F.lit(size))))
    group = [F.col(f"v.{g}").alias(g) for g in prod["group_by"]]
    measure = F.sum("revenue_gbp") if any(h == "sum" for h in prod["measures"].values()) else F.lit(0.0)
    rows = events.groupBy("window_start", *group).agg(F.count("*").alias("n"), measure.alias("sum")).collect()
    return {"windows": {(r["window_start"], tuple(r[g] for g in prod["group_by"])): {"n": r["n"], "sum": round(r["sum"] or 0.0, 4)}
                        for r in rows},
            "bad_records": bad, "events": events.count()}
