import json
import os
import time
import boto3

sqs = boto3.client("sqs")

MAIN_QUEUE_URL = os.getenv("MAIN_QUEUE_URL", "")
QUARANTINE_QUEUE_URL = os.getenv("QUARANTINE_QUEUE_URL", "")
RISK_THRESHOLD = float(os.getenv("RISK_THRESHOLD", "0.80"))

def handler(event, context):
    body = event
    if "Records" in event:  # if later wired to SQS
        body = json.loads(event["Records"][0]["body"])

    ev = normalize_event(body)
    risk = simple_risk_score(ev)

    out = {**body, "riskScore": risk, "riskTs": int(time.time())}

    if risk >= RISK_THRESHOLD:
        sqs.send_message(QueueUrl=QUARANTINE_QUEUE_URL, MessageBody=json.dumps(out))
        return {"decision": "quarantine", "riskScore": risk}

    sqs.send_message(QueueUrl=MAIN_QUEUE_URL, MessageBody=json.dumps(out))
    return {"decision": "allow", "riskScore": risk}

def normalize_event(d: dict) -> dict:
    # ensures required keys exist
    d.setdefault("eventType", "NORMAL")
    d.setdefault("schemaVersion", "1.0")
    d.setdefault("producerId", "unknown")
    d.setdefault("correlationId", f"run-{int(time.time())}")
    d.setdefault("payload", {})
    d.setdefault("fanoutDegree", 0)
    d.setdefault("hopCount", 0)
    d.setdefault("timestamp", int(time.time()))
    # compute payloadSizeBytes if missing
    if "payloadSizeBytes" not in d:
        d["payloadSizeBytes"] = len(json.dumps(d["payload"]).encode("utf-8"))
    return d

def simple_risk_score(ev: dict) -> float:
    score = 0.05
    et = ev.get("eventType", "NORMAL")
    if et in {"POISON", "LOOP"}:
        score += 0.70
    if int(ev.get("payloadSizeBytes", 0)) > 32000:
        score += 0.20
    if int(ev.get("fanoutDegree", 0)) >= 20:
        score += 0.20
    if int(ev.get("hopCount", 0)) >= 3:
        score += 0.10
    return min(0.99, score)
