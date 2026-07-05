import hashlib
import json
import os
import time
import random
import boto3

sqs = boto3.client("sqs")
lmb = boto3.client("lambda")

MODE = os.getenv("MODE", "baseline")  # baseline | guarded
MAIN_QUEUE_URL = os.getenv("MAIN_QUEUE_URL", "")
RISK_GATE_FN = os.getenv("RISK_GATE_FN", "")  # function name
SEED = int(os.getenv("SEED", "42"))


def _emit(ev: dict):
    """Emit an event: in guarded mode via the risk gate, otherwise straight to the main queue."""
    if MODE == "guarded" and RISK_GATE_FN:
        lmb.invoke(
            FunctionName=RISK_GATE_FN,
            InvocationType="RequestResponse",
            Payload=json.dumps(ev).encode("utf-8"),
        )
    else:
        sqs.send_message(QueueUrl=MAIN_QUEUE_URL, MessageBody=json.dumps(ev))


def handler(event, context):
    # Per-run seed: different input samples across repeats give real variance and a
    # diverse ML dataset. If no seed is passed, the env SEED (=42) reproduces prior work.
    seed = int(event.get("seed", SEED))
    random.seed(seed)

    run_id = event.get("runId") or event.get("run_id") or f"run-{int(time.time())}"
    count = int(event.get("count", 100))
    profile = event.get("profile", "mixed")

    result = {"sent": count, "mode": MODE, "profile": profile, "runId": run_id, "seed": seed}

    # Population mode: a stream from many opaque-ID sources. Reputation is derived from
    # BEHAVIOUR, not identity. The role map is returned as ground truth for evaluation only.
    if profile == "population":
        producers = build_population(seed)
        weights = [p["weight"] for p in producers]
        for i in range(count):
            p = random.choices(producers, weights=weights, k=1)[0]
            ev = make_event(p["profile"], i, run_id)
            ev["producerId"] = p["pid"]   # opaque id overrides the default
            ev["genSeq"] = i              # creation order, for rolling reputation
            _emit(ev)
        result["population"] = {p["pid"]: p["role"] for p in producers}
        return result

    for i in range(count):
        _emit(make_event(profile, i, run_id))
    return result

# Masking profiles: a compromised AUTHORIZED source falsifies declared metadata to
# bypass the deterministic gate.
MASK_PROFILES = {"s1", "s2", "s3"}

# Source population for the ML dataset. Roles are seeded; ids are opaque hashes, so the
# model relies on behaviour rather than identity. weight = relative activity (stream volume).
#   (role, profile, number of sources, activity weight)
POPULATION_SPEC = [
    ("benign_normal", "normal", 12, 3),  # background: NORMAL, fan-out 0
    ("benign_fanout", "fanout", 8,  2),  # honest fan-out (declared=real), legitimate volume
    ("mal_s1",        "s1",     3,  1),  # per-event type lie
    ("mal_s2",        "s2",     3,  1),  # per-event degree lie
    ("mal_s3",        "s3",     4,  5),  # drip: high activity, attack signature is in VOLUME
]


def build_population(seed: int):
    """Deterministic source population with opaque ids (role not encoded in the id)."""
    producers = []
    i = 0
    for role, profile, n, weight in POPULATION_SPEC:
        for _ in range(n):
            pid = hashlib.sha1(f"{seed}:{i}".encode()).hexdigest()[:12]
            producers.append({"pid": pid, "role": role, "profile": profile, "weight": weight})
            i += 1
    return producers


def make_event(profile: str, idx: int, run_id: str):
    if profile in MASK_PROFILES:
        return make_mask_event(profile, idx, run_id)

    event_type = choose_type(profile)
    fanout = 0
    if event_type == "FANOUT":
        fanout = random.choice([5, 10, 20, 30])

    payload = {"i": idx, "kind": event_type, "data": "x" * random.choice([50, 200, 1000])}

    return {
        "eventType": event_type,
        "producerId": "lab-generator",
        "schemaVersion": "1.0",
        "correlationId": run_id,
        "payload": payload,
        "fanoutDegree": fanout,
        "hopCount": 0,
        "timestamp": int(time.time()),
    }


def make_mask_event(profile: str, idx: int, run_id: str):
    """Event with falsified metadata.

    Declared fields (eventType, fanoutDegree) are what the gate sees and scores; realFanout
    is the hidden true fan-out (a physical consequence of execution) that the gate does not
    see but the processor realizes. For S1/S2 the real payload uses the same distribution as
    the honest fanout profile ({5,10,20,30}), so fanout/S1/S2 differ ONLY in the declared lie,
    not in intensity. S3 (drip) is per-event honest (declared=real, small), harmful only in
    aggregate volume, which motivates the reputation feature.
    """
    if profile == "s1":
        real = random.choice([5, 10, 20, 30])
        declared_type, declared_fanout = "NORMAL", 0
    elif profile == "s2":
        real = random.choice([5, 10, 20, 30])  # same distribution as fanout/S1
        declared_type, declared_fanout = "FANOUT", random.choice([0, 5])
    else:  # s3 drip: event looks legitimate (declared=real); maliciousness is in volume
        real = random.choice([5, 10])
        declared_type, declared_fanout = "FANOUT", real  # declared = real -> no per-event gap

    payload = {"i": idx, "kind": "mask", "data": "x" * random.choice([50, 200, 1000])}

    return {
        "eventType": declared_type,            # declared (gate sees this)
        "producerId": "compromised-producer",  # authorized but malicious
        "schemaVersion": "1.0",
        "correlationId": run_id,
        "payload": payload,
        "fanoutDegree": declared_fanout,        # declared (gate sees this; understated/zero)
        "hopCount": 0,
        "timestamp": int(time.time()),
        "realFanout": real,                     # hidden (only the processor/consequence sees it)
        "maskStrategy": profile.upper(),        # ground-truth label for analysis, not a feature
    }


def choose_type(profile: str) -> str:
    if profile == "normal":
        return "NORMAL"
    if profile == "poison":
        return "POISON"
    if profile == "fanout":
        return "FANOUT"
    if profile == "loop":
        return "LOOP"
    # mixed
    r = random.random()
    if r < 0.75: return "NORMAL"
    if r < 0.85: return "POISON"
    if r < 0.95: return "FANOUT"
    return "LOOP"
