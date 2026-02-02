import json
import os
import time
import boto3

sqs = boto3.client("sqs")
ddb = boto3.resource("dynamodb")

STATE_TABLE_NAME = os.getenv("STATE_TABLE_NAME", "")
MAIN_QUEUE_URL = os.getenv("MAIN_QUEUE_URL", "")
MAX_HOPS = int(os.getenv("MAX_HOPS", "4"))

table = ddb.Table(STATE_TABLE_NAME)


MAX_HOPS = int(os.getenv("MAX_HOPS", "4"))

def handler(event, context):
    # SQS trigger -> Records
    for rec in event.get("Records", []):
        body = json.loads(rec["body"])
        ev = normalize_event(body)

        # Persist minimal state (idempotency/counters)
        pk = f"ev#{ev.correlation_id}#{ev.event_id}"
        now = int(time.time())
        ttl = now + 3600

        # Read ApproxReceiveCount for retry signal
        approx_receive = int(rec.get("attributes", {}).get("ApproximateReceiveCount", "1"))

        # Scenario behavior
        if ev.event_type == "POISON":
            _write_state(pk, ttl, ev, approx_receive, outcome="fail_poison")
            raise RuntimeError("Poison payload simulated failure")

        if ev.event_type == "SLOW":
            time.sleep(0.2)  # keep small for cost safety

        if ev.event_type == "FANOUT":
            n = max(0, min(ev.fanout_degree or 10, 50))
            for i in range(n):
                child = _child_event(ev, suffix=f"fanout{i}", hop_inc=1, event_type="NORMAL")
                sqs.send_message(QueueUrl=MAIN_QUEUE_URL, MessageBody=json.dumps(child))

        if ev.event_type == "LOOP":
            if ev.hop_count < MAX_HOPS:
                child = _child_event(ev, suffix="loop", hop_inc=1, event_type="LOOP")
                sqs.send_message(QueueUrl=MAIN_QUEUE_URL, MessageBody=json.dumps(child))

        _write_state(pk, ttl, ev, approx_receive, outcome="success")
    return {"ok": True}
    
def normalize_event(d: dict) -> dict:
    d.setdefault("eventType", "NORMAL")
    d.setdefault("schemaVersion", "1.0")
    d.setdefault("producerId", "unknown")
    d.setdefault("correlationId", f"run-{int(time.time())}")
    d.setdefault("payload", {})
    d.setdefault("fanoutDegree", 0)
    d.setdefault("hopCount", 0)
    d.setdefault("timestamp", int(time.time()))
    if "payloadSizeBytes" not in d:
        d["payloadSizeBytes"] = len(json.dumps(d["payload"]).encode("utf-8"))
    return d


def _child_event(parent: Event, suffix: str, hop_inc: int, event_type: str):
    return {
        "eventType": event_type,
        "producerId": parent.producer_id,
        "schemaVersion": parent.schema_version,
        "correlationId": parent.correlation_id,
        "payload": {"parentEventId": parent.event_id, "suffix": suffix},
        "hopCount": parent.hop_count + hop_inc,
        "fanoutDegree": parent.fanout_degree,
        "timestamp": int(time.time())
    }

def _write_state(pk: str, ttl: int, ev: Event, approx_receive: int, outcome: str):
    table.put_item(
        Item={
            "pk": pk,
            "ttl": ttl,
            "eventType": ev.event_type,
            "correlationId": ev.correlation_id,
            "payloadSizeBytes": ev.payload_size_bytes,
            "fanoutDegree": ev.fanout_degree,
            "hopCount": ev.hop_count,
            "approxReceiveCount": approx_receive,
            "outcome": outcome,
            "ts": int(time.time()),
        }
    )
