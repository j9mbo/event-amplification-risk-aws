from __future__ import annotations
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


def handler(event, context):
    for rec in event.get("Records", []):
        body = json.loads(rec["body"])
        ev = normalize_event(body)  # ev is dict

        correlation_id = ev.get("correlationId") or "na"
        event_id = ev.get("eventId") or "na"
        pk = f"ev#{correlation_id}#{event_id}"

        ttl = int(time.time()) + 3600

        approx_receive = int(rec.get("attributes", {}).get("ApproximateReceiveCount", "1"))

        et = ev.get("eventType", "NORMAL")

        if et == "POISON":
            _write_state(pk, ttl, ev, approx_receive, outcome="fail_poison")
            raise RuntimeError("Poison payload simulated failure")

        if et == "SLOW":
            time.sleep(0.2)

        if et == "FANOUT":
            n = max(0, min(int(ev.get("fanoutDegree", 10) or 10), 50))
            for i in range(n):
                child = _child_event(ev, suffix=f"fanout{i}", hop_inc=1, event_type="NORMAL")
                sqs.send_message(QueueUrl=MAIN_QUEUE_URL, MessageBody=json.dumps(child))

        if et == "LOOP":
            if int(ev.get("hopCount", 0)) < MAX_HOPS:
                child = _child_event(ev, suffix="loop", hop_inc=1, event_type="LOOP")
                sqs.send_message(QueueUrl=MAIN_QUEUE_URL, MessageBody=json.dumps(child))

        _write_state(pk, ttl, ev, approx_receive, outcome="success")

    return {"ok": True}


def normalize_event(d: dict) -> dict:
    now = int(time.time())
    d = dict(d)  # defensive copy

    d.setdefault("eventType", "NORMAL")
    d.setdefault("schemaVersion", "1.0")
    d.setdefault("producerId", "unknown")
    d.setdefault("correlationId", f"run-{now}")
    d.setdefault("eventId", f"ev-{now}-{abs(hash(json.dumps(d, sort_keys=True))) % 10_000_000}")
    d.setdefault("payload", {})
    d.setdefault("fanoutDegree", 0)
    d.setdefault("hopCount", 0)
    d.setdefault("timestamp", now)

    if "payloadSizeBytes" not in d:
        d["payloadSizeBytes"] = len(json.dumps(d["payload"]).encode("utf-8"))

    return d


def _child_event(parent: dict, suffix: str, hop_inc: int, event_type: str) -> dict:
    return {
        "eventType": event_type,
        "producerId": parent.get("producerId", "unknown"),
        "schemaVersion": parent.get("schemaVersion", "1.0"),
        "correlationId": parent.get("correlationId", "na"),
        "eventId": f'{parent.get("eventId","na")}-{suffix}',
        "payload": {"parentEventId": parent.get("eventId", "na"), "suffix": suffix},
        "hopCount": int(parent.get("hopCount", 0)) + hop_inc,
        "fanoutDegree": int(parent.get("fanoutDegree", 0)),
        "timestamp": int(time.time()),
        "payloadSizeBytes": 0,
    }


def _write_state(pk: str, ttl: int, ev: dict, approx_receive: int, outcome: str):
    table.put_item(
        Item={
            "pk": pk,
            "ttl": ttl,
            "eventType": ev.get("eventType", "na"),
            "correlationId": ev.get("correlationId", "na"),
            "eventId": ev.get("eventId", "na"),
            "payloadSizeBytes": int(ev.get("payloadSizeBytes", 0)),
            "fanoutDegree": int(ev.get("fanoutDegree", 0)),
            "hopCount": int(ev.get("hopCount", 0)),
            "approxReceiveCount": approx_receive,
            "outcome": outcome,
            "ts": int(time.time()),
        }
    )
