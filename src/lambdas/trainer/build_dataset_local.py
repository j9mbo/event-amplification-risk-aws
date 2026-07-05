"""Build the training dataset with an offline replayer of the testbed.

The behaviour engine (gate decision R(e) + cascade emittedChildren=min(real,cap)) is taken
from the REAL testbed code and validated against hardware (simulate_stand.py: mean deviation
0.17%). The input model is CONTINUOUS in intensity (declared as such).

Malicious source family (all present in training; we hold out NOT the mechanism but unseen
INTENSITIES):
  - MASK  -- per-event understatement: actual = declared + s, s ~ [10..35] (large gap);
  - VOL   -- volumetric abuse: per-event honest (declared=actual, small), but large volume
             v ~ [60..160] events per source.
Benign: actual = declared (consistent), varied volume and magnitude (including "busy").

Held out (held_out=1, test only): MASK with s in [24,30] and VOL with v in [110,140] --
unseen intensities of EACH type. All other intensities are in training.

Run:  ./.venv/bin/python src/lambdas/trainer/build_dataset_local.py
Out:  data/maskB_dataset.csv
"""

import csv
import hashlib
import importlib.util
import os
import random
import sys
import types

_b = types.ModuleType("boto3")
_b.client = lambda *a, **k: None
_b.resource = lambda *a, **k: types.SimpleNamespace(Table=lambda *a, **k: None)
sys.modules["boto3"] = _b

ROOT = os.path.join(os.path.dirname(__file__), "..", "..", "..")
GATE_PATH = os.path.join(ROOT, "src", "lambdas", "risk_gate", "app.py")
_spec = importlib.util.spec_from_file_location("gate_app", GATE_PATH)
gate = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gate)

TAU = 0.25
CAP = 50
SEED = 20260624


def make_pid(tag, i):
    return hashlib.sha1(f"{SEED}:{tag}:{i}".encode()).hexdigest()[:12]


def build_producers(rng):
    """Source list: (pid, family, role, n_events, params, held_out)."""
    P = []
    # --- benign ---
    for i in range(100):  # normal
        P.append((make_pid("bn", i), "BENIGN", "benign_normal", 25, {}, 0))
    for i in range(75):   # fanout (varied magnitude, small volume)
        P.append((make_pid("bf", i), "BENIGN", "benign_fanout", 30, {}, 0))
    for i in range(40):   # busy (large volume, small fan-out)
        P.append((make_pid("bb", i), "BENIGN", "benign_busy", rng.randint(80, 160), {}, 0))
    # --- MASK: continuous understatement s ---
    for i in range(70):
        s = rng.randint(10, 35)
        held = 1 if 24 <= s <= 30 else 0          # held-out intensity band
        P.append((make_pid("m", i), "MASK", "mal_mask", 30, {"s": s}, held))
    # --- VOL: continuous volume v ---
    for i in range(60):
        v = rng.randint(60, 160)
        held = 1 if 110 <= v <= 140 else 0        # held-out volume band
        P.append((make_pid("v", i), "VOL", "mal_vol", v, {"v": v}, held))
    return P


def gen_event(family, params, rng):
    """(declaredType, declaredFanout, real, payload) by family/params."""
    pl = rng.randint(200, 2000)
    if family == "BENIGN":
        # consistent: declared = real; the role sets the range via real_fn in params
        real = params["real_fn"](rng)
        dt = "FANOUT" if real > 0 else "NORMAL"
        return dt, real, real, pl
    if family == "MASK":
        d = rng.randint(0, 5)
        real = d + params["s"]                     # understated by s
        return ("FANOUT" if d > 0 else "NORMAL"), d, real, pl
    # VOL -- per-event honest, small
    real = rng.randint(2, 8)
    return "FANOUT", real, real, pl


def benign_real_fn(role):
    if role == "benign_normal":
        return lambda rng: 0
    if role == "benign_fanout":
        return lambda rng: rng.randint(1, 30)
    return lambda rng: rng.randint(1, 2)           # busy


def build():
    rng = random.Random(SEED)
    producers = build_producers(rng)
    events = []
    for pid, family, role, n_ev, params, held in producers:
        p = {"pid": pid, "family": family, "role": role, "held": held}
        ev_params = dict(params)
        if family == "BENIGN":
            ev_params["real_fn"] = benign_real_fn(role)
        for _ in range(n_ev):
            dt, df, real, pl = gen_event(family, ev_params, rng)
            events.append((p, dt, df, real, pl))
    rng.shuffle(events)

    rep = {}
    rows = []
    for seq, (p, dt, df, real, pl) in enumerate(events):
        r = gate.simple_risk_score({"eventType": dt, "fanoutDegree": df,
                                    "payloadSizeBytes": pl, "hopCount": 0})
        if r >= TAU:
            continue
        emitted = min(real, CAP)
        st = rep.get(p["pid"])
        if st is None:
            f_count = 0; f_mean = f_max = f_cum = f_rate = 0.0
        else:
            f_count = st["n"]; f_mean = st["sum"] / st["n"]; f_max = st["max"]
            f_cum = st["sum"]; f_rate = st["n"] / max(1, seq - st["first"])
        rows.append({
            "declaredType": 1 if dt == "FANOUT" else 0, "declaredFanout": df,
            "payloadSize": pl, "hopCount": 0,
            "emittedChildren": emitted, "gap": emitted - df,
            "rep_count": f_count, "rep_meanEmitted": round(f_mean, 3),
            "rep_maxEmitted": f_max, "rep_cumChildren": f_cum, "rep_rate": round(f_rate, 4),
            "label": 0 if p["family"] == "BENIGN" else 1,
            "family": p["family"], "role": p["role"], "held_out": p["held"],
            "producerId": p["pid"],
        })
        if st is None:
            rep[p["pid"]] = {"n": 1, "sum": emitted, "max": emitted, "first": seq}
        else:
            st["n"] += 1; st["sum"] += emitted; st["max"] = max(st["max"], emitted)
    return rows


def main():
    rows = build()
    out_dir = os.path.join(ROOT, "data"); os.makedirs(out_dir, exist_ok=True)
    out = os.path.join(out_dir, "maskB_dataset.csv")
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)

    from collections import defaultdict
    g = defaultdict(list)
    for r in rows:
        g[(r["role"], r["held_out"])].append(r)
    pos = sum(r["label"] for r in rows)
    print(f"Rows: {len(rows)} | malicious {pos} ({100*pos//len(rows)}%) -> {out}")
    print(f"\n{'role':14} {'held':>4} {'events':>6} {'sources':>7} {'gap':>5} {'rep_mean':>8} {'rep_cum':>8}")
    print("-" * 60)
    for (role, held), rs in sorted(g.items()):
        npid = len(set(x["producerId"] for x in rs))
        print(f"{role:14} {held:>4} {len(rs):>6} {npid:>7} "
              f"{sum(x['gap'] for x in rs)/len(rs):>5.1f} "
              f"{sum(x['rep_meanEmitted'] for x in rs)/len(rs):>8.1f} "
              f"{sum(x['rep_cumChildren'] for x in rs)/len(rs):>8.1f}")


if __name__ == "__main__":
    main()
