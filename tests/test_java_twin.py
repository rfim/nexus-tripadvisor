"""The Java job reaches exactly the Python job's windows and ledger on the same week of page_views."""
import csv
import json
import shutil
import subprocess

import pytest

from helpers import site
from nexus.paths import LANDING, ROOT, TARGET


def test_java_matches_python_window_for_window():
    if not shutil.which("javac"):
        pytest.skip("needs a JDK")
    site()
    subprocess.run(["javac", "-d", str(TARGET / "java"), *map(str, (ROOT / "java/src/nexus").glob("*.java"))], check=True)
    out = TARGET / "java_page_views.csv"
    subprocess.run(["java", "-cp", str(TARGET / "java"), "nexus.WindowJob", str(LANDING / "kafka/page_views"), "6", str(out)],
                   check=True, capture_output=True)
    py = json.loads((TARGET / "stream_page_views.json").read_text())
    jv = [[r["window_start"], r["location_id"], int(r["views"])] for r in csv.DictReader(out.open())]
    assert jv == py["windows"]
    led = json.loads((TARGET / "java_page_views.ledger.json").read_text())
    assert {k: led[k] for k in ("read", "accepted", "duplicates", "late_dropped", "late_updates")} == \
           {k: py["ledger"][k] for k in ("read", "accepted", "duplicates", "late_dropped", "late_updates")}
