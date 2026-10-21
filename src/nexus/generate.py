"""Simulate a week of a travel site's events, written as Kafka would keep them: one log per partition.

Three topics: page_views, partner_clicks (the "check prices" clicks partners pay for) and reviews.
Each record is {"offset", "ts" (append time), "key", "value" (the producer's JSON, as a string)}.
Six problems are planted, the ones I've met running streams in production; PLANTED says where.

Deterministic: the same seed writes the same bytes.
"""
import json
import random
import zlib
from datetime import datetime, timedelta, timezone

from nexus.contracts import topic
from nexus.paths import LANDING

START = datetime(2026, 10, 5, tzinfo=timezone.utc)
DAYS = 7
END = START + timedelta(days=DAYS)
CITIES = ["London", "Paris", "Rome", "Barcelona", "Lisbon", "Edinburgh", "Amsterdam", "New York"]
KINDS = ["hotel", "restaurant", "attraction"]
PARTNERS = ["partner_a", "partner_b", "partner_c", "partner_d", "partner_e", "partner_f"]

PLANTED = {
    "retries": {"topic": "page_views", "from": "2026-10-06T11:30:00Z", "to": "2026-10-06T13:00:00Z", "share": 0.8},
    "offline_upload": {"topic": "page_views", "events_from": "2026-10-07T10:00:00Z", "events_to": "2026-10-07T15:30:00Z",
                       "delivered_at": "2026-10-07T18:00:00Z", "sessions": 40},
    "schema_v2": {"topic": "partner_clicks", "from": "2026-10-07T09:00:00Z", "app_version": "7.4"},
    "poison": {"topic": "partner_clicks", "partition": 3, "at": "2026-10-09T14:05:12Z"},
    "bots": {"topic": "page_views", "from": "2026-10-10T03:00:00Z", "to": "2026-10-10T04:00:00Z", "users": 3, "views": 1500},
    "erasure": {"topic": "reviews", "at": "2026-10-10T11:00:00Z", "stale_edit_arrives": "2026-10-10T11:02:00Z"},
}


def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def parse(iso: str) -> int:
    return ms(datetime.fromisoformat(iso.replace("Z", "+00:00")))


def iso(t: int) -> str:
    return datetime.fromtimestamp(t / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{t % 1000:03d}Z"


def partition(key: str, n: int) -> int:
    return zlib.crc32(key.encode()) % n


def locations() -> list[dict]:
    out, n = [], 0
    for city in CITIES:
        for kind in KINDS:
            for i in range(4):
                n += 1
                out.append({"location_id": f"d{186000 + n * 37}", "city": city, "kind": kind,
                            "name": f"{city} {kind} {i + 1}"})
    return out


def _diurnal(hour: int) -> float:
    """Sessions per hour, London-centred: quiet at night, peaks at lunch and in the evening."""
    curve = [3, 2, 1.5, 1.2, 1.2, 1.6, 3, 5, 7, 8, 8.5, 9, 10, 9.5, 9, 9, 9.5, 10, 11, 12, 12, 10, 7, 5]
    return curve[hour]


class Log:
    """Records per topic before offsets are assigned; offsets follow append time within a partition."""

    def __init__(self):
        self.records = {t: [] for t in ("page_views", "partner_clicks", "reviews")}

    def add(self, topic_name: str, key: str, append: int, value, raw: str | None = None):
        self.records[topic_name].append((append, key, raw if raw is not None else json.dumps(value, separators=(",", ":"))))

    def write(self) -> dict:
        counts = {}
        for t, recs in self.records.items():
            n = topic(t)["partitions"]
            parts = {p: [] for p in range(n)}
            for append, key, raw in recs:
                parts[partition(key, n)].append((append, key, raw))
            d = LANDING / "kafka" / t
            d.mkdir(parents=True, exist_ok=True)
            counts[t] = {}
            for p, rows in parts.items():
                rows.sort(key=lambda r: (r[0], r[1], r[2]))
                with open(d / f"p{p}.jsonl", "w") as fh:
                    for off, (append, key, raw) in enumerate(rows):
                        fh.write(json.dumps({"offset": off, "ts": iso(append), "key": key, "value": raw},
                                            separators=(",", ":")) + "\n")
                counts[t][p] = len(rows)
        return counts


def generate(seed: int = 7) -> dict:
    rnd = random.Random(seed)
    locs = locations()
    log = Log()
    truth = {"page_views": {}, "partner_clicks": {}, "bot_views": 0, "late_offline": 0, "retried": 0}
    ev = {"n": 0}

    def eid(prefix):
        ev["n"] += 1
        return f"{prefix}{ev['n']:07d}"

    def delay(platform):
        if platform == "web":
            return rnd.randint(100, 3000)
        if platform == "android" and rnd.random() < 0.02:          # flaky mobile networks: up to 55 minutes late
            return rnd.randint(11 * 60_000, 55 * 60_000)
        return rnd.randint(500, 10_000)

    retry = PLANTED["retries"]
    r_from, r_to = parse(retry["from"]), parse(retry["to"])
    v2_from = parse(PLANTED["schema_v2"]["from"])
    users = [f"u{rnd.getrandbits(40):010x}" for _ in range(9000)]
    sessions = 0

    for h in range(DAYS * 24):
        hour = START + timedelta(hours=h)
        weekend = hour.weekday() >= 5
        n_sessions = int(_diurnal(hour.hour) * (1.25 if weekend else 1.0) * 5 + rnd.random() * 4)
        for _ in range(n_sessions):
            sessions += 1
            sid = f"s{sessions:06d}"
            platform = rnd.choices(["web", "ios", "android"], [0.55, 0.25, 0.20])[0]
            app_version = "web" if platform == "web" else ("7.4" if ms(hour) >= v2_from and rnd.random() < 0.7 else "7.3")
            user = rnd.choice(users)
            t = ms(hour) + rnd.randint(0, 3_599_000)
            home = rnd.choice(locs)
            for _ in range(rnd.choices([1, 2, 3, 4, 6, 8], [20, 25, 20, 15, 12, 8])[0]):
                loc = home if rnd.random() < 0.6 else rnd.choice(locs)
                page = {"hotel": "hotel", "restaurant": "restaurant", "attraction": "attraction"}[loc["kind"]] \
                    if rnd.random() < 0.8 else "search"
                e = {"event_id": eid("pv"), "event_time": iso(t), "session_id": sid, "user_hash": user,
                     "location_id": loc["location_id"], "page_type": page, "platform": platform, "app_version": app_version}
                append = t + delay(platform)
                log.add("page_views", sid, append, e)
                day = iso(t)[:10]
                truth["page_views"][day] = truth["page_views"].get(day, 0) + 1
                if platform == "web" and r_from <= append < r_to and rnd.random() < retry["share"]:   # 1. SDK retry
                    log.add("page_views", sid, append + rnd.randint(2000, 20_000), e)
                    truth["retried"] += 1
                if loc["kind"] == "hotel" and page == "hotel" and rnd.random() < 0.22:
                    ct = t + rnd.randint(5_000, 90_000)
                    partner = rnd.choices(PARTNERS, [30, 25, 15, 12, 10, 8])[0]
                    cpc = round(rnd.uniform(0.18, 1.35), 2)
                    c = {"click_id": eid("ck"), "event_time": iso(ct), "session_id": sid, "location_id": loc["location_id"],
                         "partner": partner, "platform": platform}
                    if app_version == "7.4":                                     # 3. schema v2 from the 7.4 app
                        cur = rnd.choices(["GBP", "EUR", "USD"], [60, 30, 10])[0]
                        fx = topic("partner_clicks")["_fx"][cur]
                        c["price"] = {"amount": round(cpc / fx, 2), "currency": cur}
                    else:
                        c["cpc_gbp"] = cpc
                    log.add("partner_clicks", sid, ct + delay(platform), c)
                    cday = iso(ct)[:10]
                    truth["partner_clicks"][cday] = truth["partner_clicks"].get(cday, 0) + 1
                t += rnd.randint(8_000, 240_000)

    # 2. an offline upload: a flight's worth of iOS sessions, sent in one go hours after they happened
    off = PLANTED["offline_upload"]
    e_from, e_to, sent = parse(off["events_from"]), parse(off["events_to"]), parse(off["delivered_at"])
    for i in range(off["sessions"]):
        sid = f"s9{i:05d}"
        user, t = rnd.choice(users), rnd.randint(e_from, e_to - 1_800_000)
        for _ in range(rnd.randint(2, 8)):
            loc = rnd.choice(locs)
            e = {"event_id": eid("pv"), "event_time": iso(t), "session_id": sid, "user_hash": user,
                 "location_id": loc["location_id"], "page_type": loc["kind"], "platform": "ios", "app_version": "7.4"}
            log.add("page_views", sid, sent + rnd.randint(0, 15 * 60_000), e)
            truth["page_views"][iso(t)[:10]] += 1
            truth["late_offline"] += 1
            t += rnd.randint(20_000, 300_000)

    # 5. a scraper: three users, a burst of views on a handful of locations, in the quietest hour
    bots = PLANTED["bots"]
    b_from, b_to = parse(bots["from"]), parse(bots["to"])
    bot_users = [f"u{rnd.getrandbits(40):010x}" for _ in range(bots["users"])]
    targets = rnd.sample(locs, 10)
    for i in range(bots["views"]):
        t = b_from + rnd.randint(0, b_to - b_from - 1)
        sid = f"sb{i % 30:04d}"
        e = {"event_id": eid("pv"), "event_time": iso(t), "session_id": sid, "user_hash": bot_users[i % 3],
             "location_id": rnd.choice(targets)["location_id"], "page_type": "hotel", "platform": "web", "app_version": "web"}
        log.add("page_views", sid, t + rnd.randint(100, 900), e)
        truth["page_views"][iso(t)[:10]] += 1
        truth["bot_views"] += 1

    # 4. a poison pill: one truncated record on partner_clicks partition 3
    poison = PLANTED["poison"]
    p_at = parse(poison["at"])
    key = next(f"s{k:06d}" for k in range(1, 10**6) if partition(f"s{k:06d}", 4) == poison["partition"])
    good = {"click_id": eid("ck"), "event_time": iso(p_at - 1500), "session_id": key, "location_id": locs[0]["location_id"],
            "partner": "partner_b", "platform": "web", "cpc_gbp": 0.64}
    log.add("partner_clicks", key, p_at, None, raw=json.dumps(good, separators=(",", ":"))[:57])

    # 6. reviews: submitted, moderated, sometimes edited; one reviewer erases theirs and a stale edit arrives after
    review_truth = _reviews(rnd, log, locs, users, eid)

    counts = log.write()
    truth["reviews"] = review_truth
    truth["records"] = counts
    truth["planted"] = PLANTED | {"poison_key": key, "bot_users": bot_users,
                                  "bot_locations": sorted(t["location_id"] for t in targets)}
    (LANDING / "truth.json").write_text(json.dumps(truth, indent=1, sort_keys=True))
    (LANDING / "locations.json").write_text(json.dumps(locs, indent=1))
    return truth


def _reviews(rnd, log, locs, users, eid) -> dict:
    er = PLANTED["erasure"]
    erase_at, stale_at = parse(er["at"]), parse(er["stale_edit_arrives"])
    final, who = {}, {}
    n = 0
    for h in range(DAYS * 24):
        hour = START + timedelta(hours=h)
        for _ in range(int(_diurnal(hour.hour) * 0.9 + rnd.random() * 2)):
            n += 1
            rid, loc, user = f"r{n:05d}", rnd.choice(locs), rnd.choice(users)
            t = ms(hour) + rnd.randint(0, 3_599_000)
            base = {"review_id": rid, "location_id": loc["location_id"], "reviewer_hash": user,
                    "reviewer_email": f"{user[1:9]}@example.com", "lang": rnd.choice(["en", "en", "en", "fr", "it", "es"])}
            rating = rnd.choices([1, 2, 3, 4, 5], [5, 7, 15, 33, 40])[0]
            versions = [(t, {"op": "submitted", "rating": rating, "status": "pending", "text_chars": rnd.randint(80, 1800)})]
            t2 = t + rnd.randint(120_000, 3_600_000)
            versions.append((t2, {"op": "moderated", "rating": rating, "status": "published" if rnd.random() < 0.92 else "rejected",
                                  "text_chars": versions[0][1]["text_chars"]}))
            if versions[-1][1]["status"] == "published" and rnd.random() < 0.15:
                t3 = t2 + rnd.randint(3_600_000, 48 * 3_600_000)
                if t3 < ms(END):
                    versions.append((t3, {"op": "edited", "rating": max(1, min(5, rating + rnd.choice([-1, 1]))),
                                          "status": "published", "text_chars": rnd.randint(80, 1800)}))
            for v, (vt, body) in enumerate(versions, 1):
                e = {"event_id": eid("rv"), "event_time": iso(vt), "version": v, **base, **body}
                log.add("reviews", rid, vt + rnd.randint(200, 4000), e)
            final[rid] = {"version": len(versions), "rating": versions[-1][1]["rating"], "status": versions[-1][1]["status"],
                          "location_id": loc["location_id"]}
            who[rid] = base
    # the erasure: a published review edited twice, erased, and the earlier edit re-sent by a retrying producer
    rid = next(r for r, f in sorted(final.items()) if f["status"] == "published" and f["version"] == 2
               and int(r[1:]) < n // 2)
    f = final[rid]
    t_edit = erase_at - 3_600_000
    edit = {"event_id": eid("rv"), "event_time": iso(t_edit), "version": 3, **who[rid], "op": "edited",
            "rating": 2, "status": "published", "text_chars": 640}
    log.add("reviews", rid, t_edit + 900, edit)
    tomb = {"event_id": eid("rv"), "event_time": iso(erase_at), "version": 4, "review_id": rid, "op": "erased"}
    log.add("reviews", rid, erase_at + 700, tomb)
    log.add("reviews", rid, stale_at, edit | {"event_id": eid("rv")})          # an outbox replay re-sends version 3
    final[rid] = {"version": 4, "erased": True, "location_id": f["location_id"]}
    return {"reviews": len(final), "erased": [rid], "erased_reviewer": who[rid]["reviewer_hash"], "final": final}
