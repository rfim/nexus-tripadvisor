"""Turn pytest's JUnit XML into assets/data/tests.json, the report the site shows."""
import json
import sys
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime, timezone

SPARK = {"test_spark_batch"}
LOCAL = {"test_java_twin", "test_flink"}
ABOUT = {
    "test_generate": "The simulated topics: deterministic, every planted problem really in the log",
    "test_contracts": "Schema v1 and v2 read as one; the registry would reject v2; no guessing on bad records",
    "test_stream": "Watermarks, idle partitions, lateness, dedupe, dead letters, crash and restore, tombstones",
    "test_flink": "Apache Flink 1.20 on a MiniCluster: the Python job's windows, the batch's revenue, the poison pill dead-lettered",
    "test_java_twin": "The Java job reaches exactly the Python job's windows and ledger (JDK 17)",
    "test_spark_batch": "The PySpark batch matches the plain-Python batch window for window",
    "test_batch": "Every stream/batch difference explained; only the scraper hour held",
    "test_sla": "Freshness from published completeness: the incident is the only breach",
    "test_warehouse": "Snowflake and BigQuery SQL from one contract; masking, merge, erasure",
    "test_data": "The published run keeps its promises",
    "test_benchmarks": "Each decision beats its alternatives in the published benchmarks",
    "test_site": "Pages link to real files; every element the scripts fill exists",
}


def main(xml_path: str, out: str) -> None:
    root = ET.parse(xml_path).getroot()
    suite = root if root.tag == "testsuite" else root.find("testsuite")
    files = defaultdict(list)
    for tc in suite.iter("testcase"):
        mod = tc.get("classname", "").split(".")[-1]
        outcome = "failed" if tc.find("failure") is not None or tc.find("error") is not None else \
                  "skipped" if tc.find("skipped") is not None else "passed"
        files[mod].append({"name": tc.get("name"), "outcome": outcome, "seconds": round(float(tc.get("time", 0)), 3)})
    report = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
              "total": sum(len(v) for v in files.values()),
              **{k: sum(t["outcome"] == k for v in files.values() for t in v) for k in ("passed", "failed", "skipped")},
              "seconds": round(float(suite.get("time", 0)), 1),
              "suites": [{"file": f"tests/{m}.py", "about": ABOUT.get(m, ""), "spark": m in SPARK, "java": m in LOCAL, "tests": t}
                         for m, t in sorted(files.items(), key=lambda kv: list(ABOUT).index(kv[0]) if kv[0] in ABOUT else 99)]}
    with open(out, "w") as fh:
        json.dump(report, fh, indent=1)
    print(f"{report['passed']} passed, {report['failed']} failed, {report['skipped']} skipped → {out}")


if __name__ == "__main__":
    main(*sys.argv[1:3])
