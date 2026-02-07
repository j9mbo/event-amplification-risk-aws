import json
import os
import time
import random
import boto3

sqs = boto3.client("sqs")
lmb = boto3.client("lambda")

MODE = os.getenv("MODE", "baseline")  # baseline|guarded
MAIN_QUEUE_URL = os.getenv("MAIN_QUEUE_URL", "")
RISK_GATE_FN = os.getenv("RISK_GATE_FN", "")  # function name
SEED = int(os.getenv("SEED", "42"))

def handler(event, context):
    random.seed(SEED)

    # Basic configurable batch
    count = int(event.get("count", 100))
    profile = event.get("profile", "mixed")  # normal|poison|fanout|loop|mixed
    run_id = event.get("runId") or f"run-{int(time.time())}"

    for i in range(count):
        run_id = event.get("runId") or f"run-{int(time.time())}"
        if MODE == "guarded" and RISK_GATE_FN:
            lmb.invoke(
                FunctionName=RISK_GATE_FN,
                InvocationType="RequestResponse",
                Payload=json.dumps(ev).encode("utf-8"),
            )
        else:
            sqs.send_message(QueueUrl=MAIN_QUEUE_URL, MessageBody=json.dumps(ev))

    return {"sent": count, "mode": MODE, "profile": profile}

def make_event(profile: str, idx: int, run_id: str):
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
