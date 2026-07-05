"""Closed loop (model in the loop) -- measure AF after source quarantine.

Goal: turn the caveat "only DETECTION is proven, not enforcement" into a concrete number.
A frozen RF B model is placed in the enforcement loop, and the amplification factor (AF) is
measured on the same validated offline twin used for the ML results (deviation 0.00%/0.17%).

Honest framing (kept in the paper):
  - The model is POST-INGRESS: `emittedChildren` is known only AFTER processing, so the flagged
    event has already amplified and cannot be rolled back. Enforcement is PER-SOURCE: once the
    aggregate score of a source crosses the FROZEN threshold (chosen on validation at FPR<=1%),
    all of that source's SUBSEQUENT events are quarantined (0 children).
  - Containment is therefore partial: a residual LEAK of events passes before the trigger
    (~1 event/source for MASK; a ~5-event cold-start window for VOL).
  - The engine (gate decision R(e) + cascade emittedChildren=min(real,cap) + rolling reputation)
    and the stream are IDENTICAL to build_dataset_local.py -> nothing changed but enforcement.

Protocol matches train_local.py: split by source (split3), RF B on B_FEATURES, threshold frozen
on validation at FPR<=1%, 5 split seeds, mean +/- sigma. Held-out intensities (held_out=1) test only.

AF = (admitted + children) / all inputs -- as in step 1 (simulate_stand.simulate_run).
A quarantined event (by gate OR model): 0 children, not "admitted", but present in the denominator.

Run:  ./.venv/bin/python src/lambdas/trainer/closed_loop_local.py
"""
import os
import random
import sys

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

HERE = os.path.dirname(__file__)
sys.path.insert(0, HERE)
import build_dataset_local as B          # engine + population + generation (validated)
from train_local import split3, frozen_threshold, B_FEATURES

ROOT = os.path.join(HERE, "..", "..", "..")
CSV = os.path.join(ROOT, "data", "maskB_dataset.csv")
SEEDS = [11, 22, 33, 44, 55]
TARGET_FPR = 0.01
CAP = B.CAP          # gate-only cap (50) -- physical limit on children per event
TAU = B.TAU


# ----------------------------- FULL EVENT STREAM -----------------------------

def build_stream():
    """Regenerate the IDENTICAL stream of build_dataset_local.build(), but keep EVERY event
    (including gate-quarantined) plus causal reputation features and the gate decision.
    Returns a list of per-event records in global stream order."""
    rng = random.Random(B.SEED)
    producers = B.build_producers(rng)
    events = []
    for pid, family, role, n_ev, params, held in producers:
        p = {"pid": pid, "family": family, "role": role, "held": held}
        ev_params = dict(params)
        if family == "BENIGN":
            ev_params["real_fn"] = B.benign_real_fn(role)
        for _ in range(n_ev):
            dt, df, real, pl = B.gen_event(family, ev_params, rng)
            events.append((p, dt, df, real, pl))
    rng.shuffle(events)                  # same shuffle as build()

    rep = {}
    recs = []
    for seq, (p, dt, df, real, pl) in enumerate(events):
        r = B.gate.simple_risk_score({"eventType": dt, "fanoutDegree": df,
                                      "payloadSizeBytes": pl, "hopCount": 0})
        gate_admit = r < TAU
        emitted = min(real, CAP)         # children IF the event is actually processed
        st = rep.get(p["pid"])
        if st is None:
            f_count = 0; f_mean = f_max = f_cum = f_rate = 0.0
        else:
            f_count = st["n"]; f_mean = st["sum"] / st["n"]; f_max = st["max"]
            f_cum = st["sum"]; f_rate = st["n"] / max(1, seq - st["first"])
        feats = {
            "declaredType": 1 if dt == "FANOUT" else 0, "declaredFanout": df,
            "payloadSize": pl, "hopCount": 0,
            "emittedChildren": emitted, "gap": emitted - df,
            "rep_count": f_count, "rep_meanEmitted": round(f_mean, 3),
            "rep_maxEmitted": f_max, "rep_cumChildren": f_cum, "rep_rate": round(f_rate, 4),
        }
        recs.append({
            "seq": seq, "pid": p["pid"], "family": p["family"], "role": p["role"],
            "held_out": p["held"], "label": 0 if p["family"] == "BENIGN" else 1,
            "gate_admit": gate_admit, "real": real, "emitted": emitted, "feats": feats,
        })
        # reputation updates ONLY for gate-admitted events (as in build())
        if gate_admit:
            if st is None:
                rep[p["pid"]] = {"n": 1, "sum": emitted, "max": emitted, "first": seq}
            else:
                st["n"] += 1; st["sum"] += emitted; st["max"] = max(st["max"], emitted)
    return recs


def verify_against_csv(recs, df_csv):
    """Consistency guard: gate-admitted records must exactly match the dataset."""
    adm = [r for r in recs if r["gate_admit"]]
    assert len(adm) == len(df_csv), f"admitted {len(adm)} != csv {len(df_csv)}"
    pos_rec = sum(r["label"] for r in adm)
    pos_csv = int(df_csv.label.sum())
    assert pos_rec == pos_csv, f"pos {pos_rec} != csv {pos_csv}"
    # check feature sums (emitted + reputation) -- stream is identical
    s_rec = sum(r["feats"]["emittedChildren"] for r in adm)
    s_csv = int(df_csv.emittedChildren.sum())
    assert s_rec == s_csv, f"emitted sum {s_rec} != csv {s_csv}"
    return len(adm), pos_rec


# --------------------------- ENFORCEMENT LOOP + AF ---------------------------

def af_configs(test_recs, score):
    """One pass over the test stream (global order) -> AF in three configs, plus absolute
    children (DoW), leak (cold-start) and false suppression of legitimate children.

    `score` maps seq -> model decision (precomputed in a batch; an event's score depends only
    on its CAUSAL features, which enforcement does not affect, so it can be computed ahead).

    Configs (children cap = CAP in all, to isolate the CONTRIBUTION of gate/model):
      undef  -- no defense: every event emits min(real,CAP);
      gate   -- deterministic gate only (tier 0): admitted emit, quarantined 0;
      gateML -- gate + PER-SOURCE model loop: block an admitted event if the source is already
                flagged; otherwise emit, then score, and if score>=thr flag the source
                (its subsequent events are blocked)."""
    fams = ["BENIGN", "MASK", "VOL"]
    U = {f: [0, 0, 0] for f in fams}            # [n_input, admitted, children]
    G = {f: [0, 0, 0] for f in fams}
    M = {f: [0, 0, 0] for f in fams}
    flagged = set()
    leak_children = {"MASK": 0.0, "VOL": 0.0}   # children that leaked BEFORE flagging
    leak_events = {"MASK": 0, "VOL": 0}
    children_undef = {"MASK": 0.0, "VOL": 0.0}  # absolute children with no defense (DoW baseline)
    false_suppressed = 0                        # legitimate children suppressed by a false flag

    for r in sorted(test_recs, key=lambda x: x["seq"]):
        fam = r["family"]; em = r["emitted"]; ga = r["gate_admit"]
        if fam in children_undef:
            children_undef[fam] += em
        # --- undefended ---
        U[fam][0] += 1; U[fam][1] += 1; U[fam][2] += em
        # --- gate-only ---
        G[fam][0] += 1
        if ga:
            G[fam][1] += 1; G[fam][2] += em
        # --- gate + ML loop ---
        M[fam][0] += 1
        if not ga:
            pass                                  # quarantined by the gate
        elif r["pid"] in flagged:
            if r["label"] == 0:
                false_suppressed += em            # legitimate event suppressed by a false flag
        else:
            M[fam][1] += 1; M[fam][2] += em       # event processed (amplified)
            if r["label"] == 1:                    # leak before the trigger (cold-start)
                leak_children[fam] += em; leak_events[fam] += 1
            if score[r["seq"]] >= 0:               # source flagged (threshold applied in score)
                flagged.add(r["pid"])             # enforce on this source's SUBSEQUENT events

    def af(d, fam):
        n, adm, ch = d[fam]
        return (adm + ch) / n if n else float("nan")

    def af_all(d):
        n = sum(d[f][0] for f in fams); adm = sum(d[f][1] for f in fams)
        ch = sum(d[f][2] for f in fams)
        return (adm + ch) / n if n else float("nan")

    return {
        "af": {cfg: {**{f: af(d, f) for f in fams}, "ALL": af_all(d)}
               for cfg, d in [("undef", U), ("gate", G), ("gateML", M)]},
        "leak_children": leak_children, "leak_events": leak_events,
        "children_undef": children_undef, "false_suppressed": false_suppressed,
        "mal_producers": {f: len({r["pid"] for r in test_recs
                                  if r["family"] == f and r["held_out"] == 1})
                          for f in ["MASK", "VOL"]},
    }


def main():
    recs = build_stream()
    df = pd.read_csv(CSV)
    n_adm, n_pos = verify_against_csv(recs, df)
    print(f"Dataset check: gate-admitted {n_adm} (= csv), malicious {n_pos} -- OK")
    print(f"Full stream: {len(recs)} events (including gate-quarantined)\n")
    print(f"Protocol: split3 by source; RF B; threshold frozen on val FPR<={TARGET_FPR}; "
          f"seeds: {len(SEEDS)}. Children cap CAP={CAP}.\n")

    pid_recs = {}
    for r in recs:
        pid_recs.setdefault(r["pid"], []).append(r)

    acc = {cfg: {f: [] for f in ["BENIGN", "MASK", "VOL", "ALL"]}
           for cfg in ["undef", "gate", "gateML"]}
    leak_ev = {"MASK": [], "VOL": []}
    ch_undef = {"MASK": [], "VOL": []}          # absolute children with no defense / source
    ch_leak = {"MASK": [], "VOL": []}            # absolute children with the loop / source
    fsupp = []

    for seed in SEEDS:
        train, val, test = split3(df, seed)
        model = RandomForestClassifier(n_estimators=300, n_jobs=-1, random_state=seed)
        model.fit(train[B_FEATURES], train.label)
        thr = frozen_threshold(model.predict_proba(val[B_FEATURES])[:, 1],
                               val.label.values, TARGET_FPR)
        # test stream: all events of test sources (in global seq order)
        test_pids = set(test.producerId.unique())
        test_recs = [r for p in test_pids for r in pid_recs[p]]
        # batch scoring (gate-admitted events only -- others are not scored)
        scored = [r for r in test_recs if r["gate_admit"]]
        X = pd.DataFrame([r["feats"] for r in scored])[B_FEATURES]
        proba = model.predict_proba(X)[:, 1]
        score = {r["seq"]: (1.0 if p >= thr else -1.0) for r, p in zip(scored, proba)}
        out = af_configs(test_recs, score)
        for cfg in ["undef", "gate", "gateML"]:
            for f in ["BENIGN", "MASK", "VOL", "ALL"]:
                acc[cfg][f].append(out["af"][cfg][f])
        for f in ["MASK", "VOL"]:
            npr = max(1, out["mal_producers"][f])
            leak_ev[f].append(out["leak_events"][f] / npr)
            ch_undef[f].append(out["children_undef"][f] / npr)
            ch_leak[f].append(out["leak_children"][f] / npr)
        fsupp.append(out["false_suppressed"])

    def ms(xs):
        xs = [x for x in xs if not (isinstance(x, float) and np.isnan(x))]
        return f"{np.mean(xs):6.2f}±{np.std(xs):.2f}"

    print("AF by config (mean +/- sigma over 5 splits):")
    print(f"{'family':>8} {'no defense':>14} {'gate only':>14} {'gate+ML loop':>16}")
    print("-" * 56)
    for f in ["MASK", "VOL", "BENIGN", "ALL"]:
        print(f"{f:>8} {ms(acc['undef'][f]):>14} {ms(acc['gate'][f]):>14} {ms(acc['gateML'][f]):>16}")

    print("\nAbsolute blast radius -- children per malicious source:")
    print(f"{'family':>8} {'no defense':>16} {'leaked (loop)':>18} {'reduction':>10}")
    print("-" * 56)
    for f in ["MASK", "VOL"]:
        u, l = np.mean(ch_undef[f]), np.mean(ch_leak[f])
        red = 100 * (u - l) / u if u else 0
        print(f"{f:>8} {ms(ch_undef[f]):>16} {ms(ch_leak[f]):>18} {red:>9.1f}%")

    print("\nLeak (cold-start) -- malicious-source events that passed BEFORE flagging:")
    for f in ["MASK", "VOL"]:
        print(f"  {f}: {ms(leak_ev[f])} events per source (then quarantined)")
    print(f"\nFalse suppression of legitimate children (from false benign flags): "
          f"{np.mean(fsupp):.0f}±{np.std(fsupp):.0f} children / test stream")

    g = {f: np.mean(acc['gate'][f]) for f in ['MASK', 'VOL']}
    m = {f: np.mean(acc['gateML'][f]) for f in ['MASK', 'VOL']}
    print("\nLoop containment (AF reduction gate-only -> gate+ML):")
    for f in ["MASK", "VOL"]:
        red = 100 * (g[f] - m[f]) / g[f] if g[f] else 0
        print(f"  {f}: {g[f]:.2f} -> {m[f]:.2f}  (-{red:.1f} %)")


if __name__ == "__main__":
    main()
