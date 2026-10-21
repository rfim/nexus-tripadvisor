"""Event time, the watermark, allowed lateness and de-duplication on six records. Edit the times and run it again."""
import json

from nexus.generate import parse
from nexus.stream import Job, Record


def pv(eid, event_time):
    return json.dumps({"event_id": eid, "event_time": event_time, "session_id": "s", "user_hash": "u",
                       "location_id": "d186037", "page_type": "hotel", "platform": "ios", "app_version": "7.4"})


rows = [  # arrival,               partition, offset, value
    ("2026-10-05T10:05:00.000Z", 0, 0, pv("a", "2026-10-05T10:04:00.000Z")),
    ("2026-10-05T10:05:09.000Z", 0, 1, pv("a", "2026-10-05T10:04:00.000Z")),   # an SDK retry: same event_id
    ("2026-10-05T11:12:00.000Z", 0, 2, pv("b", "2026-10-05T11:11:00.000Z")),   # takes the watermark past 11:00
    ("2026-10-05T11:30:00.000Z", 0, 3, pv("c", "2026-10-05T10:58:00.000Z")),   # late, within the hour allowed
    ("2026-10-05T12:30:00.000Z", 0, 4, pv("d", "2026-10-05T12:29:00.000Z")),
    ("2026-10-05T12:31:00.000Z", 0, 5, pv("e", "2026-10-05T10:30:00.000Z")),   # two hours late: side output
]
job = Job("page_views")
job.watch = {"a", "b", "c", "d", "e"}
job.run([Record(parse(t), p, o, "s", v) for t, p, o, v in rows])
for d in job.decisions:
    print(f"{d['arrived'][11:19]}  {d['id']}  watermark {str(d['wm'])[11:19]:8}  {d['decision']}")
print("\nwindows:", [(r["window_start"][11:16], r["n"], f"fired {r['emits']}x") for r in job.rows()])
print("side output:", list(job.late), "· ledger:", job.s.ledger)
