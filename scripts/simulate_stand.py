"""Offline replayer of the testbed behaviour (hybrid approach).

Reproduces the deployed testbed logic WITHOUT AWS, reusing the REAL generator code
(make_event/build_population) and gate (simple_risk_score), so deviation from hardware is
minimal. The cascade follows the processor/app.py rules:
  - admit if R(e) < tau; otherwise quarantine (not processed);
  - an admitted event emits min(real_fanout, cap) children.

Purpose:
  1. validate() -- compare the replayer's AF against real step-1 runs (runs/maskB_*.json)
     for profiles whose code did not change (fanout, s1).
  2. the same engine feeds the large-scale dataset (build_dataset_local.py).

Run the check:  python3 scripts/simulate_stand.py
"""

import glob
import importlib.util
import json
import os
import random
import re
import sys
import types

# --- boto3 stub, to import the lambda code without AWS ---
_b = types.ModuleType("boto3")
_b.client = lambda *a, **k: None
_b.resource = lambda *a, **k: types.SimpleNamespace(Table=lambda *a, **k: None)
sys.modules["boto3"] = _b

ROOT = os.path.join(os.path.dirname(__file__), "..")
LAMBDAS = os.path.join(ROOT, "src", "lambdas")


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m

gen = _load("gen_app", os.path.join(LAMBDAS, "generator", "app.py"))
gate = _load("gate_app", os.path.join(LAMBDAS, "risk_gate", "app.py"))


def effective_real_fanout(ev: dict) -> int:
    """How many children the event REALLY emits (as in processor/app.py)."""
    if "realFanout" in ev:
        return int(ev["realFanout"])
    if ev.get("eventType") == "FANOUT":
        return int(ev.get("fanoutDegree", 0))
    return 0


def simulate_run(profile: str, n: int, seed: int, tau: float, cap: int):
    """Replay one run of one profile. Returns admitted/quarantined/processed/AF."""
    random.seed(seed)
    admitted = quarantined = children = 0
    for i in range(n):
        ev = gen.make_event(profile, i, "sim")
        r = gate.simple_risk_score(gate.normalize_event(dict(ev)))
        if r >= tau:
            quarantined += 1
            continue
        admitted += 1
        children += min(effective_real_fanout(ev), cap)
    processed = admitted + children
    return {
        "admitted": admitted, "quarantined": quarantined,
        "processed": processed, "af": round(processed / n, 2) if n else 0.0,
    }


# ----------------------- VALIDATION AGAINST REAL RUNS -----------------------

def validate():
    """Compare the replayer's AF against real step-1 run files (fanout, s1)."""
    runs = glob.glob(os.path.join(ROOT, "runs", "maskB_*.json"))
    # only profiles whose code did NOT change after step 1
    keep = ("fanout", "s1")
    real = {}
    for f in runs:
        d = json.load(open(f))
        m = re.search(r"maskB_(\w+?)_(gateOnly|fullDef)_.*_seed(\d+)", d["runId"])
        if not m:
            continue
        prof, cfg, seed = m.group(1), m.group(2), int(m.group(3))
        if prof not in keep:
            continue
        real[(prof, cfg, seed)] = d["io"]["amplification"]

    if not real:
        print("No real fanout/s1 runs to compare against.")
        return

    cap_of = {"gateOnly": 50, "fullDef": 5}
    print(f"{'profile':8} {'config':9} {'seed':>7} {'AF real':>10} {'AF model':>10} {'d%':>7}")
    print("-" * 60)
    diffs = []
    for (prof, cfg, seed), af_real in sorted(real.items()):
        sim = simulate_run(prof, 50, seed, tau=0.25, cap=cap_of[cfg])
        af_sim = sim["af"]
        d = 100 * (af_sim - af_real) / af_real if af_real else 0
        diffs.append(abs(d))
        print(f"{prof:8} {cfg:9} {seed:>7} {af_real:>10.2f} {af_sim:>10.2f} {d:>6.1f}%")
    print("-" * 60)
    print(f"mean |d| = {sum(diffs)/len(diffs):.2f}%   max |d| = {max(diffs):.2f}%   (n={len(diffs)})")
    print("SQS delivery noise in real runs is a few %; the replayer is deterministic.")


if __name__ == "__main__":
    validate()
