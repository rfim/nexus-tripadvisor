"""One reader for every schema version, and a poison pill it refuses to guess about."""
from nexus.contracts import BadRecord, compatible, topic, upcast

v1 = '{"click_id":"ck1","event_time":"2026-10-07T08:59:00.000Z","partner":"partner_a","cpc_gbp":0.64}'
v2 = '{"click_id":"ck2","event_time":"2026-10-07T09:01:00.000Z","partner":"partner_a","price":{"amount":0.74,"currency":"EUR"}}'
for raw in (v1, v2):
    print(upcast("partner_clicks", raw)["revenue_gbp"], "GBP from", raw[:60], "…")

s = topic("partner_clicks")["schemas"]
print("\nIs v2 backward compatible with v1?", compatible(s[1], s[2]) or "yes")

try:
    upcast("partner_clicks", '{"click_id":"ck0023948","event_time":"2026-10-09T14:05:10')
except BadRecord as e:
    print("\nPoison pill:", e, "→ the dead-letter topic, not a crash")
