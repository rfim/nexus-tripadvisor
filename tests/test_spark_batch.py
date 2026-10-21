"""The PySpark batch matches the plain-Python batch window for window."""
import pytest

from helpers import site


@pytest.mark.parametrize("name", ["page_views", "partner_clicks"])
def test_spark_matches_python(name):
    pytest.importorskip("pyspark")
    from nexus import batch, spark_batch
    from nexus.stream import read_topic
    site()
    spark = spark_batch.get_spark("nexus-tests")
    a, b = spark_batch.recompute(spark, name), batch.recompute(name, read_topic(name))
    assert a["windows"].keys() == b["windows"].keys()
    assert all(a["windows"][k]["n"] == w["n"] and abs(a["windows"][k]["sum"] - w["sum"]) < 1e-6 for k, w in b["windows"].items())
    assert (a["bad_records"], a["events"]) == (b["bad_records"], b["events"])
