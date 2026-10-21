# Incident: partner_revenue_15m late for 79 minutes

**Severity:** SEV-2 (tier-1 product late, no data lost)  
**Started:** 2026-10-09T14:05:12.000Z · **Detected:** 2026-10-09T14:35:00.000Z · **Mitigated:** 2026-10-09T15:20:00.000Z · **Recovered:** 2026-10-09T15:25:00.000Z

## What happened

One record on `partner_clicks` partition 3, offset 171, was 57 bytes of truncated JSON. The stream job failed on it, restored from its checkpoint, read it again and failed again: 74 restarts in 74 minutes. Nothing downstream moved, so `partner_revenue_15m` aged to 90 minutes against a 45-minute SLA.

## Impact

- 11 fifteen-minute revenue windows (13 clicks, £8.68) were published late. None were wrong once published.
- Data lost: 1 click: the record itself, truncated by the producer; held in the DLQ.

## Timeline (UTC)

- **14:05** · job: Record 3/171 can't be parsed. The job fails, restores from its checkpoint, reads the same record, and fails again.
- **14:08** · would page: Restart count passes 3 in 10 minutes. Not alerted on yet: this is action 2.
- **14:35** · SLA monitor: partner_revenue_15m is 50 minutes old, over its 45-minute SLA. Page sent.
- **14:39** · on call: Acknowledged. Job RESTARTING; exception points to partition 3 offset 171.
- **15:20** · on call: Mitigation: deploy with the dead-letter topic on. Restore from the last checkpoint; the bad record goes to the DLQ.
- **15:25** · SLA monitor: Back within SLA. Every delayed window re-computed from the checkpoint.

## Root cause

The job treated a record it couldn't parse as a reason to stop. A restart can't fix a bad record: it is still at the same offset after every restore. The cause upstream is the producer cutting the record short.

## Why it took 29 minutes to notice

Only freshness was alerted on, and freshness is a lagging signal: it has to exceed the SLA first. The job's restart count said the same thing within three minutes.

## Actions

- [x] Dead-letter bad records by default in the shared job template (done: stream.Job(dlq=True)).
- [ ] Page on restarts > 3 in 10 minutes: detection in 3 minutes instead of 29.
- [ ] Alert on DLQ depth > 0 for tier-1 topics, with the record and its producer.
- [ ] Producer contract test in the partner SDK's CI: every record must parse under the registered schema.
