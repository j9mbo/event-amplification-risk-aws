import os

APP_NAME = os.getenv("APP_NAME", "event-amplification-risk")
STATE_TABLE_NAME = os.getenv("STATE_TABLE_NAME", "")
MAIN_QUEUE_URL = os.getenv("MAIN_QUEUE_URL", "")
QUARANTINE_QUEUE_URL = os.getenv("QUARANTINE_QUEUE_URL", "")
DLQ_URL = os.getenv("DLQ_URL", "")
MODE = os.getenv("MODE", "baseline")  # baseline|guarded
RISK_THRESHOLD = float(os.getenv("RISK_THRESHOLD", "0.80"))
