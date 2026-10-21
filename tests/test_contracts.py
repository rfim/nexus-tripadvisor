"""The schema registry's view: v2 is not backward compatible, so the reader upcasts both versions."""
import pytest

from nexus.contracts import BadRecord, compatible, topic, upcast


def test_v2_would_be_rejected_by_a_backward_compatibility_check():
    s = topic("partner_clicks")["schemas"]
    assert compatible(s[1], s[2]) == ["field 'price' is new in v2 and absent from v1 data", "field 'cpc_gbp' was removed"]


@pytest.mark.parametrize("raw, gbp", [
    ('{"click_id":"c","event_time":"2026-10-07T09:00:00.000Z","cpc_gbp":0.5}', 0.5),
    ('{"click_id":"c","event_time":"2026-10-07T09:00:00.000Z","price":{"amount":1.0,"currency":"EUR"}}', 0.86),
    ('{"click_id":"c","event_time":"2026-10-07T09:00:00.000Z","price":{"amount":1.0,"currency":"USD"}}', 0.79),
])
def test_both_versions_read_as_revenue_in_gbp(raw, gbp):
    assert upcast("partner_clicks", raw)["revenue_gbp"] == gbp


def test_an_unknown_currency_is_a_dead_letter_not_a_guess():
    with pytest.raises(BadRecord):
        upcast("partner_clicks", '{"event_time":"2026-10-07T09:00:00.000Z","price":{"amount":1,"currency":"JPY"}}')
