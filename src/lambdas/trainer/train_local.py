"""Training and held-out evaluation (full disciplined protocol).

Evaluation discipline:
  - split by SOURCE into train / val / test (no leakage);
  - hold out unseen INTENSITIES of known types (MASK s in [24,30], VOL v in [110,140]) -> test;
  - the decision threshold is chosen on VALIDATION at a target FPR and FROZEN for the test;
  - several split seeds -> mean +/- sigma.
Models: a plain threshold (no training), Logistic and RandomForest, each in variant A (no
reputation) and B (+ reputation). Event-level and source-level metrics plus cold start.

Honest framing: "unseen variants of known abuses", NOT "a new attack type". The loop is post-ingress.

Run:  ./.venv/bin/python src/lambdas/trainer/train_local.py
"""
import os
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline

ROOT = os.path.join(os.path.dirname(__file__), "..", "..", "..")
CSV = os.path.join(ROOT, "data", "maskB_dataset.csv")
SEEDS = [11, 22, 33, 44, 55]
TARGET_FPR = 0.01   # threshold frozen on validation at this FPR

A_FEATURES = ["declaredType", "declaredFanout", "payloadSize", "hopCount", "emittedChildren", "gap"]
REP = ["rep_count", "rep_meanEmitted", "rep_maxEmitted", "rep_cumChildren", "rep_rate"]
B_FEATURES = A_FEATURES + REP


def split3(df, seed):
    """Sources: benign 50/20/30; seen-malicious 70/30 (train/val);
    held-out malicious (held_out=1) -> test only."""
    rng = np.random.RandomState(seed)
    tr, va, te = set(), set(), set()
    for role, g in df[df.label == 0].groupby("role"):
        p = sorted(g.producerId.unique()); rng.shuffle(p)
        n = len(p); a, b = int(.5 * n), int(.7 * n)
        tr.update(p[:a]); va.update(p[a:b]); te.update(p[b:])
    seen = df[(df.label == 1) & (df.held_out == 0)]
    for fam, g in seen.groupby("family"):
        p = sorted(g.producerId.unique()); rng.shuffle(p)
        k = int(.7 * len(p)); tr.update(p[:k]); va.update(p[k:])
    te.update(df[(df.label == 1) & (df.held_out == 1)].producerId.unique())
    pick = lambda S: df[df.producerId.isin(S)].copy()
    return pick(tr), pick(va), pick(te)


def frozen_threshold(proba, y, target_fpr):
    """Threshold that yields FPR<=target on validation (quantile of benign probabilities)."""
    ben = proba[y == 0]
    return float(np.quantile(ben, 1 - target_fpr)) if len(ben) else 0.5


def metrics(test, pred):
    held = test.held_out == 1
    out = {}
    for fam in ["MASK", "VOL"]:
        m = (held & (test.family == fam)).values
        out[f"rec_{fam}"] = float(pred[m].mean()) if m.sum() else np.nan
    ben = (test.label == 0).values
    out["FPR_ev"] = float(pred[ben].mean()) if ben.sum() else np.nan
    # source level
    t = test.assign(pred=pred)
    bp = t[t.label == 0].groupby("producerId").pred.max()
    out["FPR_prod"] = float((bp > 0).mean())
    mp = t[(t.label == 1) & (t.held_out == 1)].groupby("producerId").pred.max()
    out["rec_prod"] = float((mp > 0).mean())
    return out


def coldstart(test, pred):
    """Recall on the first events of held-out VOL (reputation not yet accumulated)."""
    t = test.assign(pred=pred)
    v = t[(t.family == "VOL") & (t.held_out == 1)]
    res = {}
    for label, cond in [("1st", v.rep_count == 0), ("first 3", v.rep_count < 3),
                        ("first 5", v.rep_count < 5), (">=5", v.rep_count >= 5)]:
        s = v[cond]
        res[label] = (float(s.pred.mean()) if len(s) else np.nan, len(s))
    return res


def build_models():
    return {
        "Logistic A": (make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000)), A_FEATURES),
        "Logistic B": (make_pipeline(StandardScaler(), LogisticRegression(max_iter=2000)), B_FEATURES),
        "RF A":       (RandomForestClassifier(n_estimators=300, n_jobs=-1), A_FEATURES),
        "RF B":       (RandomForestClassifier(n_estimators=300, n_jobs=-1), B_FEATURES),
    }


def main():
    df = pd.read_csv(CSV)
    print(f"Sources: {df.producerId.nunique()} | events: {len(df)} | "
          f"malicious {int((df.label==1).mean()*100)}%")
    print(f"Protocol: train/val/test by source; threshold frozen on validation at FPR<={TARGET_FPR}; "
          f"split seeds: {len(SEEDS)}\n")

    agg = {name: {k: [] for k in ["rec_MASK", "rec_VOL", "FPR_ev", "FPR_prod", "rec_prod"]}
           for name in list(build_models()) + ["Threshold emitted>10"]}
    cold = {k: [] for k in ["1st", "first 3", "first 5", ">=5"]}

    for seed in SEEDS:
        train, val, test = split3(df, seed)
        # baseline: fixed rule
        pred = (test.emittedChildren > 10).astype(int).values
        for k, v in metrics(test, pred).items():
            agg["Threshold emitted>10"][k].append(v)
        # trained models with a frozen threshold
        for name, (model, feats) in build_models().items():
            if hasattr(model, "set_params"):
                try: model.set_params(random_state=seed)
                except Exception: pass
            model.fit(train[feats], train.label)
            thr = frozen_threshold(model.predict_proba(val[feats])[:, 1], val.label.values, TARGET_FPR)
            pred = (model.predict_proba(test[feats])[:, 1] >= thr).astype(int)
            for k, v in metrics(test, pred).items():
                agg[name][k].append(v)
            if name == "RF B":
                for kk, (val_, _n) in coldstart(test, pred).items():
                    cold[kk].append(val_)

    def ms(xs):
        xs = [x for x in xs if not np.isnan(x)]
        return f"{np.mean(xs):.3f}±{np.std(xs):.3f}"

    print(f"{'method':16} {'rec MASK':>11} {'rec VOL':>11} {'FPR event':>11} {'FPR src':>11} {'rec src':>11}")
    print("-" * 76)
    for name in ["Threshold emitted>10", "Logistic A", "Logistic B", "RF A", "RF B"]:
        a = agg[name]
        print(f"{name:16} {ms(a['rec_MASK']):>11} {ms(a['rec_VOL']):>11} "
              f"{ms(a['FPR_ev']):>11} {ms(a['FPR_prod']):>11} {ms(a['rec_prod']):>11}")

    print("\nCold start (RF B, held-out VOL) -- recall by event position of a new source:")
    for k in ["1st", "first 3", "first 5", ">=5"]:
        print(f"  {k:8}: {ms(cold[k])}")
    print("\nSummary: MASK is caught immediately (per-event signal); VOL only once reputation has "
          "accumulated -> low recall on a new source's first event (cold start). FPR is on honest fan-out.")


if __name__ == "__main__":
    main()
