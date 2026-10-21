"""A week of a travel site's events, written the way Kafka keeps them: one log per partition."""
from nexus.generate import generate
from nexus.paths import LANDING

truth = generate()
for topic, parts in truth["records"].items():
    print(f"{topic:15} {sum(parts.values()):6,} records in {len(parts)} partitions: {dict(parts)}")
print(f"\nretried by the SDK: {truth['retried']}, uploaded hours late: {truth['late_offline']}, bot views: {truth['bot_views']}")
print("\nthe first two records of partner_clicks partition 3:")
print("".join(open(LANDING / "kafka/partner_clicks/p3.jsonl").readlines()[:2]))
