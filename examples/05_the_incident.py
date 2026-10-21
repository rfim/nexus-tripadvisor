"""The poison pill, without a dead-letter topic: the crash loop, the SLA breach, and the postmortem."""
from nexus import sla
from nexus.contracts import topic
from nexus.generate import END, START, generate, ms, parse
from nexus.stream import Job, read_topic

generate()
job = Job("partner_clicks", dlq=False).run(read_topic("partner_clicks"), fix_at=parse("2026-10-09T15:20:00Z"))
series = sla.age_series(job, ms(START), ms(END))
inc = sla.incident(job, series, topic("partner_clicks")["product"])
for t, age in inc["series"][::3]:
    print(f"{t[11:16]}  {'█' * int(age // 3):32} {age:4.0f} min{'  ← over SLA' if age > 45 else ''}")
print()
print(sla.postmortem(inc))
