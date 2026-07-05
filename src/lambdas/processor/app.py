from __future__ import annotations
import json
import os
import time
import boto3

sqs = boto3.client("sqs")
ddb = boto3.resource("dynamodb")

STATE_TABLE_NAME = os.getenv("STATE_TABLE_NAME", "")
MAIN_QUEUE_URL = os.getenv("MAIN_QUEUE_URL", "")
QUARANTINE_QUEUE_URL = os.getenv("QUARANTINE_QUEUE_URL", "")

MODE = os.getenv("MODE", "baseline")  # baseline | guarded

MAX_HOPS = int(os.getenv("MAX_HOPS", "4"))
MAX_FANOUT = int(os.getenv("MAX_FANOUT", "5"))  # hard cap for FANOUT children in guarded

table = ddb.Table(STATE_TABLE_NAME)


def handler(event, context):
    for rec in event.get("Records", []):
        body = json.loads(rec["body"])
        ev = normalize_event(body)

        correlation_id = ev.get("correlationId") or "na"
        event_id = ev.get("eventId") or "na"
        pk = f"ev#{correlation_id}#{event_id}"

        ttl = int(time.time()) + 3600
        approx_receive = int(rec.get("attributes", {}).get("ApproximateReceiveCount", "1"))

        et = ev.get("eventType", "NORMAL")
        emitted = 0  # actual number of children emitted (behavioural signal)

        # --- POISON: execution-failure path (leave to SQS redrive / DLQ)
        if et == "POISON":
            _write_state(pk, ttl, ev, approx_receive, outcome="fail_poison")
            raise RuntimeError("Poison payload simulated failure")

        if et == "SLOW":
            time.sleep(0.2)

        # --- MASKING: real fan-out hidden from the gate. Declared fields are benign (the gate
        # admitted the event), but it really emits realFanout children. The processor realizes
        # the physical consequence the gate never saw, because the gate scored declared metadata.
        if "realFanout" in ev:
            requested = max(0, min(int(ev.get("realFanout", 0) or 0), 50))
            n = requested
            if MODE == "guarded":
                n = min(requested, MAX_FANOUT)  # processor cap: the weaker, second line
                if requested > MAX_FANOUT:
                    _write_state(pk, ttl, ev, approx_receive, outcome=f"bounded_mask_truncated:{requested}->{n}")
            for i in range(n):
                child = _child_event(ev, suffix=f"mask{i}", hop_inc=1, event_type="NORMAL")
                sqs.send_message(QueueUrl=MAIN_QUEUE_URL, MessageBody=json.dumps(child))
            emitted = n

        # --- FANOUT: bounded emission in guarded mode (honest profile, no masking)
        if et == "FANOUT" and "realFanout" not in ev:
            requested = max(0, min(int(ev.get("fanoutDegree", 10) or 10), 50))
            n = requested

            if MODE == "guarded":
                n = min(requested, MAX_FANOUT)
                if requested > MAX_FANOUT:
                    # record the fact of truncation (paper-grade evidence)
                    _write_state(pk, ttl, ev, approx_receive, outcome=f"bounded_fanout_truncated:{requested}->{n}")

            for i in range(n):
                child = _child_event(ev, suffix=f"fanout{i}", hop_inc=1, event_type="NORMAL")
                sqs.send_message(QueueUrl=MAIN_QUEUE_URL, MessageBody=json.dumps(child))
            emitted = n

        # --- LOOP: bounded depth in guarded mode
        if et == "LOOP":
            hop = int(ev.get("hopCount", 0))
            if hop < MAX_HOPS:
                child = _child_event(ev, suffix="loop", hop_inc=1, event_type="LOOP")
                sqs.send_message(QueueUrl=MAIN_QUEUE_URL, MessageBody=json.dumps(child))
                emitted = 1
            else:
                if MODE == "guarded" and QUARANTINE_QUEUE_URL:
                    # optional: quarantine terminal loop events
                    sqs.send_message(QueueUrl=QUARANTINE_QUEUE_URL, MessageBody=json.dumps(ev))
                    _write_state(pk, ttl, ev, approx_receive, outcome=f"bounded_loop_quarantined:hop={hop}")
                    continue
                elif MODE == "guarded":
                    _write_state(pk, ttl, ev, approx_receive, outcome=f"bounded_loop_dropped:hop={hop}")
                    continue

        # Default success for non-bounded cases
        _write_state(pk, ttl, ev, approx_receive, outcome="success", emitted=emitted)

    return {"ok": True}


def normalize_event(d: dict) -> dict:
    now = int(time.time())
    d = dict(d)

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


def _write_state(pk: str, ttl: int, ev: dict, approx_receive: int, outcome: str, emitted: int = 0):
    item = {
        "pk": pk,
        "ttl": ttl,
        "eventType": ev.get("eventType", "na"),
        "correlationId": ev.get("correlationId", "na"),
        "eventId": ev.get("eventId", "na"),
        "producerId": ev.get("producerId", "unknown"),
        "payloadSizeBytes": int(ev.get("payloadSizeBytes", 0)),
        # declared (gate sees this) -- the model must learn NOT to trust it
        "fanoutDegree": int(ev.get("fanoutDegree", 0)),
        "hopCount": int(ev.get("hopCount", 0)),
        # actual behavioural signal -- real number of children emitted
        "emittedChildren": int(emitted),
        "approxReceiveCount": approx_receive,
        "outcome": outcome,
        "ts": int(time.time()),
    }
    # stream creation order (for rolling per-source reputation)
    if "genSeq" in ev:
        item["genSeq"] = int(ev.get("genSeq", 0))
    # ground-truth masking labels (analysis only, NOT model features)
    if "realFanout" in ev:
        item["realFanout"] = int(ev.get("realFanout", 0))
    if "maskStrategy" in ev:
        item["maskStrategy"] = ev.get("maskStrategy")
    table.put_item(Item=item)
