from dataclasses import dataclass
from typing import Any, Dict, Optional
import json
import time
import uuid

@dataclass
class Event:
    event_id: str
    event_type: str
    producer_id: str
    schema_version: str
    correlation_id: str
    payload: Dict[str, Any]
    payload_size_bytes: int
    hop_count: int
    fanout_degree: int
    ts: int

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Event":
        payload = d.get("payload", {})
        raw_payload = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        return Event(
            event_id=str(d.get("eventId") or d.get("event_id") or uuid.uuid4()),
            event_type=str(d["eventType"]),
            producer_id=str(d.get("producerId", "unknown")),
            schema_version=str(d.get("schemaVersion", "1.0")),
            correlation_id=str(d.get("correlationId", d.get("correlation_id", uuid.uuid4()))),
            payload=payload,
            payload_size_bytes=int(d.get("payloadSizeBytes", len(raw_payload))),
            hop_count=int(d.get("hopCount", 0)),
            fanout_degree=int(d.get("fanoutDegree", 0)),
            ts=int(d.get("timestamp", int(time.time()))),
        )
