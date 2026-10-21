"""The deck's "why this, not that" claims hold in the published benchmarks (assets/data/benchmarks.json)."""
import json

from nexus.paths import ROOT

B = json.loads((ROOT / "assets/data/benchmarks.json").read_text())


def test_the_chosen_watermark_never_breaches_and_waiting_for_every_partition_does():
    assert B["watermark"]["active_min"]["sla_breach_samples"] == 0 < B["watermark"]["min_all"]["sla_breach_samples"]


def test_sixty_minutes_of_lateness_absorbs_all_organic_lateness():
    lt = B["allowed_lateness"]
    assert lt["0"]["missed_vs_batch"] > lt["15"]["missed_vs_batch"] > lt["60"]["missed_vs_batch"]
    assert lt["180"]["peak_open_windows"] > lt["60"]["peak_open_windows"]


def test_every_ttl_catches_the_retries_and_bigger_ones_cost_keys():
    t = B["dedupe_ttl"]
    assert len({v["duplicates_dropped"] for v in t.values()}) == 1
    assert t["24"]["peak_dedupe_keys"] > t["1"]["peak_dedupe_keys"] > t["0.25"]["peak_dedupe_keys"]


def test_dead_letters_cost_nothing_and_keep_the_record():
    b = B["bad_record"]
    assert b["dead_letter"]["minutes_late"] == 0 and b["dead_letter"]["records_kept_for_inspection"] == 1
    assert b["fail_and_restart"]["restarts"] > 0 and b["skip_silently"]["records_kept_for_inspection"] == 0


def test_completeness_freshness_has_no_false_pages_and_the_naive_one_does():
    f = B["freshness_metric"]
    assert f["published_completeness"]["pages"] == 0 < f["last_non_empty_window"]["pages"]


def test_the_chosen_volume_rule_holds_only_the_scraper():
    v = B["volume_rule"]["2σ and ×2 (chosen)"]
    assert v["caught_scraper"] and v["false_holds"] == 0 and B["volume_rule"]["2σ"]["false_holds"] > 0
