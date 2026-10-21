"""Apache Flink 1.20 (flink/job) on a local MiniCluster reaches the same windows as the Python job and the batch."""
import csv
import json
import shutil
import subprocess

import pytest

from helpers import site
from nexus.paths import ROOT, TARGET

JOB = ROOT / "flink/job"


def run_flink() -> dict:
    """Build the job with Maven, then run NexusLocal on the week in landing/ as a plain JVM."""
    if not shutil.which("mvn") or not shutil.which("java"):
        pytest.skip("needs Maven and a JDK")
    subprocess.run(["mvn", "-q", "-B", "-f", str(JOB / "pom.xml"), "compile", "dependency:build-classpath",
                    f"-Dmdep.outputFile={JOB / 'target/cp.txt'}"], check=True, capture_output=True)
    cp = f"{JOB / 'target/classes'}:{(JOB / 'target/cp.txt').read_text().strip()}"
    subprocess.run(["java", "--add-opens", "java.base/java.util=ALL-UNNAMED", "--add-opens", "java.base/java.lang=ALL-UNNAMED",
                    "-cp", cp, "nexus.flink.NexusLocal", str(ROOT)], check=True, capture_output=True)
    return json.loads((TARGET / "flink_ledger.json").read_text())


def _rows(name):
    return {(r["window_start"].replace("Z", "")[:19], r["group"]): (int(r["n"]), float(r["sum"]))
            for r in csv.DictReader((TARGET / f"flink_{name}.csv").open())}


def test_flink_matches_the_python_job_window_for_window():
    site()
    led = run_flink()["location_views_hourly"]
    py = json.loads((TARGET / "stream_page_views.json").read_text())
    assert {(w[0][:19], w[1]): w[2] for w in py["windows"]} == {k: v[0] for k, v in _rows("location_views_hourly").items()}
    assert (led["duplicates"], led["late_dropped"], led["events_in_windows"]) == \
           (py["ledger"]["duplicates"], py["ledger"]["late_dropped"], py["ledger"]["accepted"])


def test_flink_dead_letters_the_poison_pill_and_agrees_with_the_batch():
    site()
    led = run_flink()["partner_revenue_15m"]
    assert led["dead_letters"] == 1 and "partition 3 offset 171" in led["dead_letter_records"][0]
    batch = {(w[0][:19], w[1]): (w[2], w[3]) for w in json.loads((TARGET / "batch_partner_clicks.json").read_text())}
    flink = _rows("partner_revenue_15m")
    assert flink.keys() == batch.keys() and all(flink[k][0] == n and abs(flink[k][1] - s) < 1e-6 for k, (n, s) in batch.items())
