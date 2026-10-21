"""The whole week in plain Python: three jobs, a crash, a poison pill, the batch, the checks, the incident."""
from nexus.pipeline import run

s = run()
for name, st in s["streams"].items():
    print(f"{name:15} {st['ledger']}")
print("\nTaskManager lost and restored; sink identical to a run without the crash:", s["replay"]["identical"])
for name, r in s["reconcile"].items():
    print(f"stream vs batch, {name}: {r['equal']} windows equal, {r['explained_by_side_output']} explained by the side output, "
          f"{r['unexplained']} unexplained")
print("held hours:", [(h["hour"][:13], h["views"], f"threshold {h['threshold']}") for h in s["volume"]["holds"]],
      "· bots:", s["volume"]["bots"])
for r in s["sla"]:
    print(f"SLA {r['product']:22} {r['freshness_minutes']:3} min · on time {r['on_time']:.2%} · breaches {len(r['breaches'])}")
print("warehouse checks:", sum(c["status"] == "pass" for c in s["warehouse"]["checks"]), "/", len(s["warehouse"]["checks"]), "pass")
print(f"\nfinished in {s['run']['seconds']}s")
