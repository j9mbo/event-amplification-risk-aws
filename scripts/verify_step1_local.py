"""Local check of step 1 (masking profiles) -- no AWS.

Imports the REAL generator (make_event) and gate (simple_risk_score) code with a boto3 stub.
The cascade uses the same emission rules as processor/app.py. It shows:
  1. Regression: the honest fanout profile is unchanged (25/25, AF 16.6/4.30/3.0).
  2. Masking S1-S3 bypasses the gate (all admitted at tau=0.25) while the real fan-out
     produces a cascade -> gate containment degrades.

Run:  python3 scripts/verify_step1_local.py
"""

import importlib.util
import os
import random
import sys
import types

# --- boto3 stub, to import the lambda code without AWS ---
_b = types.ModuleType("boto3")
_b.client = lambda *a, **k: None
_b.resource = lambda *a, **k: types.SimpleNamespace(Table=lambda *a, **k: None)
sys.modules["boto3"] = _b

ROOT = os.path.join(os.path.dirname(__file__), "..", "src", "lambdas")


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

gen = _load("gen_app", os.path.join(ROOT, "generator", "app.py"))
gate = _load("gate_app", os.path.join(ROOT, "risk_gate", "app.py"))

TAU_ZONE_B = 0.25
SEED = 42
N = 50


def generate(profile, n=N, seed=SEED):
    """Mirror the handler: random.seed once, then make_event in a loop."""
    random.seed(seed)
    return [gen.make_event(profile, i, "run-local") for i in range(n)]


def cascade(events, tau, max_fanout):
    """Cascade by the processor/app.py rules (guarded).

    Admitted events (R(e) < tau) are processed; each emits children:
      - masking:      min(realFanout, max_fanout)
      - honest FANOUT: min(fanoutDegree, max_fanout)
    Quarantined events are not processed (not counted in processed).
    """
    admitted = quarantined = children = 0
    for ev in events:
        r = gate.simple_risk_score(gate.normalize_event(dict(ev)))
        if r >= tau:
            quarantined += 1
            continue
        admitted += 1
        if "realFanout" in ev:
            children += min(int(ev["realFanout"]), max_fanout)
        elif ev.get("eventType") == "FANOUT":
            children += min(int(ev.get("fanoutDegree", 0)), max_fanout)
    processed = admitted + children
    af = processed / len(events) if events else 0.0
    return dict(admitted=admitted, quarantined=quarantined,
                processed=processed, af=round(af, 2))


def baseline_af(events):
    """Baseline (no gate): all admitted, cap=50 (no processor cap)."""
    children = 0
    for ev in events:
        if "realFanout" in ev:
            children += min(int(ev["realFanout"]), 50)
        elif ev.get("eventType") == "FANOUT":
            children += min(int(ev.get("fanoutDegree", 0)), 50)
    return round((len(events) + children) / len(events), 2)


print("profile | baseline AF | gateOnly(cap50) | full defense(cap5) | admitted/quarantined (tau=0.25)")
print("-" * 95)
for profile in ["fanout", "s1", "s2", "s3"]:
    evs = generate(profile)
    base = baseline_af(evs)
    go = cascade(evs, TAU_ZONE_B, max_fanout=50)
    full = cascade(evs, TAU_ZONE_B, max_fanout=5)
    tag = "honest" if profile == "fanout" else "masked"
    print(f"{profile:6s} {tag} | {base:9.2f} | AF={go['af']:6.2f} | AF={full['af']:6.2f} "
          f"| {go['admitted']:2d}/{go['quarantined']:2d}")

print()
print("== Honest fanout regression (must match prior work) ==")
evs = generate("fanout")
assert baseline_af(evs) == 16.6, baseline_af(evs)
assert cascade(evs, 0.25, 50)["af"] == 4.3, cascade(evs, 0.25, 50)
assert cascade(evs, 0.25, 5)["af"] == 3.0, cascade(evs, 0.25, 5)
assert cascade(evs, 0.25, 50)["quarantined"] == 25
print("  OK: 16.6 / 4.30 / 3.0; quarantine 25/50")

print("== Masking S1: the gate must admit ALL (gateOnly AF back to 16.6) ==")
evs = generate("s1")
go = cascade(evs, 0.25, 50)
assert go["quarantined"] == 0, go
assert go["af"] == 16.6, go
print(f"  OK: admitted {go['admitted']}/50, quarantine 0, gateOnly AF={go['af']} (gate containment = 0%)")
