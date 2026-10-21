"""The stream job, with Flink's semantics in plain Python: event time, watermarks, idle partitions, allowed lateness,
keyed de-duplication, checkpoints, a dead-letter topic and an idempotent (upsert) sink.

    records (arrival order) → upcast → dedupe on id → window by event time → fire when the watermark passes
                                 ↘ dead letters                ↘ too late: side output (the nightly batch counts them)

Everything a restore must bring back lives in State, and only State is checkpointed. The sink and the side outputs
are keyed, so re-sending after a restore changes nothing: that is how exactly-once looks from outside.
The Java twin (java/src/nexus/WindowJob.java) implements the same rules; tests/test_java_twin.py compares the two.
"""
import copy
import json
from dataclasses import dataclass, field

from nexus.contracts import BadRecord, topic, upcast
from nexus.generate import iso, parse
from nexus.paths import LANDING

MIN = 60_000
TICK = MIN                    # timers fire every minute of processing time
IDLE = 5 * MIN                # a partition with nothing for 5 minutes stops holding the watermark back
CHECKPOINT_EVERY = 10 * MIN
DEDUPE_TTL = 24 * 60 * MIN
RESTART_DELAY = MIN           # fixed-delay restart strategy
NEG = -10**15


@dataclass
class Record:
    ts: int          # append time: when the record reached the broker, and when we can first read it
    partition: int
    offset: int
    key: str
    raw: str


def read_topic(name: str) -> list[Record]:
    """All partitions of a topic, merged in arrival order (the order a consumer that keeps up sees them)."""
    recs = []
    for p in range(topic(name)["partitions"]):
        with open(LANDING / "kafka" / name / f"p{p}.jsonl") as fh:
            for line in fh:
                r = json.loads(line)
                recs.append(Record(parse(r["ts"]), p, r["offset"], r["key"], r["value"]))
    recs.sort(key=lambda r: (r.ts, r.partition, r.offset))
    return recs


@dataclass
class State:
    pos: int = 0                                        # records consumed; with the merge order, this is the offsets
    wm: int = NEG
    part_max: dict = field(default_factory=dict)        # partition → max event time seen
    part_seen: dict = field(default_factory=dict)       # partition → processing time of its last record
    windows: dict = field(default_factory=dict)         # (window_start, group) → {"n", "sum", "fired"}
    current: dict = field(default_factory=dict)         # reviews: review_id → latest version (tombstones included)
    seen: dict = field(default_factory=dict)            # id → processing time, for de-duplication
    ledger: dict = field(default_factory=lambda: dict.fromkeys(
        ["read", "accepted", "duplicates", "late_dropped", "late_updates", "dead_letters", "stale_versions"], 0))
    last_tick: int = NEG


class Poison(Exception):
    def __init__(self, rec: Record, reason: str):
        super().__init__(f"partition {rec.partition} offset {rec.offset}: {reason}")
        self.rec, self.reason = rec, reason


class Job:
    """One topic's job. Call run(); read sink, emits, late, dlq and the timeline afterwards."""

    def __init__(self, topic_name: str, dlq: bool = True):
        self.t = topic(topic_name)
        self.name, self.dlq_enabled = topic_name, dlq
        prod = self.t["product"]
        self.windowed = "window_minutes" in prod
        self.ooo = 0
        if self.windowed:
            self.size = prod["window_minutes"] * MIN
            self.ooo = prod["out_of_orderness_minutes"] * MIN
            self.lateness = prod["allowed_lateness_minutes"] * MIN
            self.group_by = prod["group_by"]
            self.sum_field = next((m for m, how in prod["measures"].items() if how == "sum"), None)
        self.s = State()
        self.sink: dict = {}          # key → {"value", "first_emit", "last_emit", "emits"}
        self.emits: list = []         # (processing time, window end) for every emission, re-sends included
        self.progress: list = []      # (processing time, "complete up to"): published with the data, so an empty
                                      # window still tells consumers the product is current
        self.late: dict = {}          # id → event, the side output of records past allowed lateness
        self.dlq: dict = {}           # (partition, offset) → reason
        self.done_at: list = []       # processing time each record was last processed
        self.timeline: list = []      # what an operator would see in the job's log
        self.checkpoints: list = []
        self.watch: set = set()       # ids or (partition, offset) whose decisions are kept for the step-by-step page
        self.decisions: list = []

    def _note(self, rec: Record, rid, decision: str, now: int, **extra):
        if rid in self.watch or (rec.partition, rec.offset) in self.watch:
            self.decisions.append({"id": rid, "partition": rec.partition, "offset": rec.offset, "arrived": iso(rec.ts),
                                   "processed": iso(now), "wm": iso(self.s.wm) if self.s.wm > NEG else None,
                                   "decision": decision, "raw": rec.raw, **extra})

    # ---------- the operator ----------
    def _watermark(self, now: int) -> int:
        active = [self.s.part_max[p] for p, seen in self.s.part_seen.items() if now - seen < IDLE]
        if active:
            wm = min(active) - self.ooo
        elif self.s.part_max:           # every partition idle: let processing time carry the watermark (a heartbeat)
            wm = now - self.ooo - IDLE
        else:
            wm = NEG
        return max(self.s.wm, wm)

    def _emit(self, key, w: dict, now: int):
        value = {"n": w["n"], "sum": round(w["sum"], 4)}
        old = self.sink.get(key)
        self.sink[key] = {"value": value, "first_emit": old["first_emit"] if old else now, "last_emit": now,
                          "emits": (old["emits"] if old else 0) + 1}
        self.emits.append((now, key[0] + self.size))

    def _advance(self, now: int):
        wm = self._watermark(now)
        if wm <= self.s.wm and self.s.wm != NEG:
            return
        self.s.wm = wm
        if not self.windowed:
            return
        done = wm - wm % self.size
        if not self.progress or done > self.progress[-1][1]:
            self.progress.append((now, done))
        for key in sorted(k for k, w in self.s.windows.items() if not w["fired"] and k[0] + self.size <= wm):
            self.s.windows[key]["fired"] = True
            self._emit(key, self.s.windows[key], now)
        for key in [k for k in self.s.windows if k[0] + self.size + self.lateness <= wm]:
            del self.s.windows[key]

    def _tick(self, now: int):
        self._advance(now)
        if now % (60 * MIN) == 0:
            self.s.seen = {k: v for k, v in self.s.seen.items() if now - v < DEDUPE_TTL}
        self.s.last_tick = now

    def _process(self, rec: Record, now: int):
        st = self.s
        st.ledger["read"] += 1
        try:
            v = upcast(self.name, rec.raw)
        except BadRecord as e:
            self._note(rec, None, "dead letter" if self.dlq_enabled else "job fails", now, reason=str(e), raw=rec.raw)
            if not self.dlq_enabled:
                raise Poison(rec, str(e)) from None
            st.ledger["dead_letters"] += 1
            self.dlq[(rec.partition, rec.offset)] = str(e)
            return
        rid = v[self.t["id"]]
        if rid in st.seen:
            st.ledger["duplicates"] += 1
            self._note(rec, rid, "duplicate", now, first_seen=iso(st.seen[rid]))
            return
        st.seen[rid] = now
        et = parse(v["event_time"])
        st.part_max[rec.partition] = max(st.part_max.get(rec.partition, NEG), et)
        st.part_seen[rec.partition] = now
        if not self.windowed:
            self._apply_version(v, now, rec)
        else:
            start = et - et % self.size
            key = (start, tuple(v[g] for g in self.group_by))
            if start + self.size + self.lateness <= st.wm:
                st.ledger["late_dropped"] += 1
                self._note(rec, rid, "too late: side output", now, event_time=v["event_time"], window=iso(start),
                           closes=iso(start + self.size + self.lateness))
                self.late[rid] = {"event_time": v["event_time"], "arrived": iso(rec.ts), "window_start": iso(start),
                                  "group": list(key[1]), "value": v.get(self.sum_field, 0) if self.sum_field else 0}
            else:
                w = st.windows.setdefault(key, {"n": 0, "sum": 0.0, "fired": False})
                w["n"] += 1
                w["sum"] += v.get(self.sum_field, 0) if self.sum_field else 0
                st.ledger["accepted"] += 1
                if w["fired"]:                       # late but within allowed lateness: fire again with the update
                    st.ledger["late_updates"] += 1
                    self._emit(key, w, now)
                self._note(rec, rid, "late: window updated and re-fired" if w["fired"] else "counted", now,
                           event_time=v["event_time"], window=iso(start), group=list(key[1]), n=w["n"],
                           revenue_gbp=v.get("revenue_gbp"), schema=2 if "price" in v else 1)
        self._advance(now)

    def _apply_version(self, v: dict, now: int, rec: Record):
        st = self.s
        cur = st.current.get(v["review_id"])
        if cur and cur["version"] >= v["version"]:   # older than what we hold, or a tombstone: never resurrect
            st.ledger["stale_versions"] += 1
            self._note(rec, v["review_id"], f"stale: version {cur['version']} kept", now, version=v["version"], op=v["op"],
                       event_id=v["event_id"])
            return
        st.ledger["accepted"] += 1
        row = {"version": v["version"], "erased": v["op"] == "erased"}
        if not row["erased"]:
            row |= {k: v[k] for k in ("location_id", "rating", "status", "lang")}
        st.current[v["review_id"]] = row
        self._note(rec, v["review_id"], "applied", now, version=v["version"], op=v["op"], rating=v.get("rating"),
                   event_id=v["event_id"])
        self.sink[v["review_id"]] = {"value": row, "last_emit": now}
        self.emits.append((now, parse(v["event_time"])))

    # ---------- the runtime: timers, checkpoints, failures ----------
    def run(self, records: list[Record], crash_at: int | None = None, fix_at: int | None = None):
        """Process every record at its arrival time. crash_at: the job is lost then and restored from its last
        checkpoint. A poison pill with dlq off crash-loops the job until fix_at, when a build with the dead-letter
        topic enabled is deployed and restored from the last checkpoint."""
        self.records = records
        self.done_at = [None] * len(records)
        resume = NEG
        end = records[-1].ts + 3 * 60 * MIN
        now = records[0].ts - records[0].ts % TICK
        while True:
            try:
                self._loop(records, now, end, resume, crash_at)
                break
            except _Crash as c:
                crash_at = None
                self.timeline.append({"at": iso(c.at), "event": "job lost (TaskManager)", "detail": "restoring from checkpoint"})
                now, resume = self._restore(c.at + RESTART_DELAY)
            except Poison as p:
                restarts = (fix_at - p.rec.ts) // RESTART_DELAY
                self.timeline.append({"at": iso(p.rec.ts), "event": "job failed", "detail": str(p),
                                      "restarts_until_fix": restarts})
                self.poison = {"at": p.rec.ts, "partition": p.rec.partition, "offset": p.rec.offset, "reason": p.reason,
                               "restarts": restarts, "fix_at": fix_at}
                self.dlq_enabled = True
                self.timeline.append({"at": iso(fix_at), "event": "fix deployed", "detail": "dead-letter topic on; restore"})
                now, resume = self._restore(fix_at)
        return self

    def _restore(self, at: int):
        ck = max((c for c in self.checkpoints if c[0] <= at), key=lambda c: c[0])
        self.s = copy.deepcopy(ck[1])
        self.timeline.append({"at": iso(at), "event": "restored", "detail": f"checkpoint {iso(ck[0])}, from record {self.s.pos}"})
        return at - at % TICK, at

    def _loop(self, records, now, end, resume, crash_at):
        st = self.s
        while now <= end:
            nxt = now + TICK
            while st.pos < len(records) and records[st.pos].ts < nxt:
                rec = records[st.pos]
                t = max(rec.ts, resume)
                if crash_at is not None and t >= crash_at:
                    raise _Crash(crash_at)
                self._process(rec, t)
                self.done_at[st.pos] = t
                st.pos += 1
            if crash_at is not None and nxt > crash_at >= now:
                raise _Crash(crash_at)
            self._tick(nxt)
            if nxt % CHECKPOINT_EVERY == 0:
                self.checkpoints.append((nxt, copy.deepcopy(st)))
                self.checkpoints = self.checkpoints[-12:]
            now = nxt

    # ---------- what the monitor reads ----------
    def rows(self) -> list[dict]:
        """The sink as rows."""
        if not self.windowed:
            return [{"review_id": k, **v["value"]} for k, v in sorted(self.sink.items())]
        g = self.group_by
        return [{"window_start": iso(k[0]), **dict(zip(g, k[1])), "n": v["value"]["n"], "sum": v["value"]["sum"],
                 "emits": v["emits"], "first_emit": iso(v["first_emit"]), "last_emit": iso(v["last_emit"])}
                for k, v in sorted(self.sink.items())]


class _Crash(Exception):
    def __init__(self, at):
        super().__init__(f"crash at {iso(at)}")
        self.at = at
