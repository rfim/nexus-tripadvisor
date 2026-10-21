"""The published run (assets/data/site.json) keeps the promises the site makes."""
import json

from nexus.paths import ROOT

D = json.loads((ROOT / "assets/data/site.json").read_text())


def test_published_replay_is_identical():
    assert D["replay"]["identical"] is True


def test_published_reconciliation_has_nothing_unexplained():
    assert all(r["unexplained"] == 0 for r in D["reconcile"].values())


def test_published_checks_all_pass():
    assert all(c["status"] == "pass" for c in D["warehouse"]["checks"])


def test_published_incident_has_a_postmortem():
    md = (ROOT / "assets/data/incident.md").read_text()
    assert D["incident"]["detected"] in md and "## Actions" in md


def test_every_planted_problem_shows_in_the_trace():
    decisions = {d["decision"] for t in D["trace"].values() for d in t}
    assert {"duplicate", "late: window updated and re-fired", "too late: side output", "job fails", "dead letter",
            "stale: version 4 kept"} <= decisions
