"""The nightly batch checks the stream; the volume rule holds the bot hour and nothing else."""
import json

from helpers import site
from nexus.paths import LANDING


def test_every_difference_between_stream_and_batch_is_explained():
    for name, r in site()["reconcile"].items():
        assert r["unexplained"] == 0, name
        assert r["batch_events"] - r["stream_events"] == r["late_events"], name


def test_the_batch_counts_every_event_exactly_once():
    truth = json.loads((LANDING / "truth.json").read_text())
    assert site()["batch"]["page_views"]["events"] == sum(truth["page_views"].values())
    assert site()["batch"]["partner_clicks"]["events"] == sum(truth["partner_clicks"].values())


def test_only_the_scraper_hour_is_held():
    v = site()["volume"]
    assert [h["hour"] for h in v["holds"]] == ["2026-10-10T03:00:00.000Z"]
    assert sorted(v["bots"]) == sorted(site()["generator"]["planted"]["bot_users"])
    assert v["holds_after_filter"] == [] and v["bot_views_removed"] == site()["generator"]["bot_views"]
