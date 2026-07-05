"""Population-mode validation: offline replayer vs a real AWS run.

Offline reproduces the SAME stream (build_population + generator handler loop) and engine
(gate + min(real,cap)); real data comes from DynamoDB (per-event records with genSeq).
Compared per source: number of admitted events and the sum of actual fan-out.

Run: ./.venv/bin/python scripts/validate_population_aws.py <runId> <seed> <count> <cap>
"""
import importlib.util
import os
import random
import sys
import boto3

ROOT = os.path.join(os.path.dirname(__file__), "..")
LAMBDAS = os.path.join(ROOT, "src", "lambdas")


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m); return m

gen = _load("gen_app", os.path.join(LAMBDAS, "generator", "app.py"))
gate = _load("gate_app", os.path.join(LAMBDAS, "risk_gate", "app.py"))

TAU = 0.25


def effective_real(ev):
    if "realFanout" in ev:
        return int(ev["realFanout"])
    return int(ev.get("fanoutDegree", 0)) if ev.get("eventType") == "FANOUT" else 0


def offline(run_id, seed, count, cap):
    random.seed(seed)
    producers = gen.build_population(seed)
    weights = [p["weight"] for p in producers]
    agg = {}  # pid -> [admitted_count, sum_emitted]
    for i in range(count):
        p = random.choices(producers, weights=weights, k=1)[0]
        ev = gen.make_event(p["profile"], i, run_id)
        ev["producerId"] = p["pid"]
        r = gate.simple_risk_score(gate.normalize_event(dict(ev)))
        if r >= TAU:
            continue
        a = agg.setdefault(p["pid"], [0, 0])
        a[0] += 1; a[1] += min(effective_real(ev), cap)
    return agg


def real_from_ddb(run_id, table_name):
    ddb = boto3.resource("dynamodb", region_name="us-east-1")
    t = ddb.Table(table_name)
    agg = {}
    kwargs = {
        "FilterExpression": "correlationId = :c AND attribute_exists(genSeq)",
        "ExpressionAttributeValues": {":c": run_id},
    }
    while True:
        resp = t.scan(**kwargs)
        for it in resp.get("Items", []):
            pid = it.get("producerId", "?")
            a = agg.setdefault(pid, [0, 0])
            a[0] += 1; a[1] += int(it.get("emittedChildren", 0))
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return agg


def main():
    run_id, seed, count, cap = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
    table = os.environ["STATE_TABLE_NAME"]
    off = offline(run_id, seed, count, cap)
    real = real_from_ddb(run_id, table)

    pids = sorted(set(off) | set(real))
    off_ev = sum(v[0] for v in off.values()); off_ch = sum(v[1] for v in off.values())
    re_ev = sum(v[0] for v in real.values()); re_ch = sum(v[1] for v in real.values())
    matched = sum(1 for p in pids if off.get(p, [0, 0]) == real.get(p, [0, 0]))
    print(f"Sources: offline {len(off)}, real {len(real)}, exact match: {matched}/{len(pids)}")
    print(f"Admitted events: offline {off_ev}, real {re_ev}  (delta {re_ev-off_ev:+d})")
    print(f"Fan-out sum: offline {off_ch}, real {re_ch}  "
          f"(delta {100*(re_ch-off_ch)/off_ch:+.2f}%)")
    # top per-source discrepancies
    diffs = [(p, off.get(p, [0, 0]), real.get(p, [0, 0])) for p in pids
             if off.get(p, [0, 0]) != real.get(p, [0, 0])]
    if diffs:
        print(f"\nMismatching sources ({len(diffs)}), first 8 [pid: offline(ev,ch) vs real(ev,ch)]:")
        for p, o, r in diffs[:8]:
            print(f"  {p}: {tuple(o)} vs {tuple(r)}")
    else:
        print("\nExact match across ALL sources.")


if __name__ == "__main__":
    main()
