#!/usr/bin/env bash
set -euo pipefail

# scripts/sanity-check.sh
# Goal: prove RiskGate is in-path by forcing deny and observing messages go to QUARANTINE (not MAIN)

ENV_FILE="${ENV_FILE:-.env.guarded}"

STRICT_THR="${STRICT_THR:-0.0}"   # 0.0 => everything should be quarantined (riskScore >= 0.0 always)
DEFAULT_THR="${DEFAULT_THR:-0.80}"

COUNT="${COUNT:-20}"
PROFILE="${PROFILE:-fanout}"      # fanout/normal/mixed - any works, fanout is fine
OBS_SLEEP="${OBS_SLEEP:-10}"      # seconds after generator invoke to let gate enqueue to quarantine
PURGE_MAIN="${PURGE_MAIN:-1}"
PURGE_QUAR="${PURGE_QUAR:-1}"
PURGE_DLQ="${PURGE_DLQ:-0}"
PURGE_SLEEP_SECONDS="${PURGE_SLEEP_SECONDS:-70}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ONE="$ROOT_DIR/scripts/run_one.sh"

use_env() {
  local f="$1"
  set -a
  # shellcheck disable=SC1090
  source "$ROOT_DIR/$f"
  set +a

  # Best-effort region inference
  if [[ -z "${AWS_REGION:-}" ]]; then
    AWS_REGION="$(aws configure get region 2>/dev/null || true)"
  fi
  if [[ -z "${AWS_REGION:-}" && -n "${MAIN_QUEUE_URL:-}" ]]; then
    AWS_REGION="$(echo "$MAIN_QUEUE_URL" | sed -nE 's#https://sqs\.([a-z0-9-]+)\.amazonaws\.com/.*#\1#p')"
  fi
  export AWS_REGION
}

jq_merge_thr() {
  local fn="$1"
  local thr="$2"

  # Get current env vars
  local vars env
  vars="$(aws lambda get-function-configuration --function-name "$fn" \
           --query 'Environment.Variables' --output json)"
  # Merge threshold
  env="$(jq -c --arg thr "$thr" '.RISK_THRESHOLD=$thr | {Variables: .}' <<<"$vars")"

  aws lambda update-function-configuration \
    --function-name "$fn" \
    --environment "$env" >/dev/null
}

purge_queue() {
  local url="$1"
  local label="$2"

  if [[ -z "$url" ]]; then
    echo "!! $label URL empty, skip purge"
    return 0
  fi

  if aws sqs purge-queue --queue-url "$url" >/dev/null 2>&1; then
    echo "==> Purged $label"
    echo "==> Cooldown ${PURGE_SLEEP_SECONDS}s (SQS purge rate-limit safety)"
    sleep "$PURGE_SLEEP_SECONDS"
    return 0
  fi

  # PurgeQueueInProgress: don't loop forever, just cooldown and proceed
  echo "==> $label: PurgeQueueInProgress -> cooldown ${PURGE_SLEEP_SECONDS}s (no retry)"
  sleep "$PURGE_SLEEP_SECONDS"
}

q_count() {
  local url="$1"
  aws sqs get-queue-attributes \
    --queue-url "$url" \
    --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
    --query 'Attributes' --output json
}

main() {
  use_env "$ENV_FILE"

  echo "==> Using env=$ENV_FILE MODE=${MODE:-} AWS_REGION=${AWS_REGION:-}"
  echo "    GENERATOR_FN=${GENERATOR_FN:-}"
  echo "    RISK_GATE_FN=${RISK_GATE_FN:-}"
  echo "    MAIN_QUEUE_URL=${MAIN_QUEUE_URL:-}"
  echo "    QUARANTINE_QUEUE_URL=${QUARANTINE_QUEUE_URL:-}"
  echo "    DLQ_URL=${DLQ_URL:-}"

  # Pre snapshots
  local main_before quar_before dlq_before
  main_before="$(q_count "$MAIN_QUEUE_URL")"
  quar_before="$(q_count "$QUARANTINE_QUEUE_URL")"
  dlq_before="$(q_count "$DLQ_URL")"

  if [[ "$PURGE_MAIN" == "1" ]]; then purge_queue "$MAIN_QUEUE_URL" "MAIN"; fi
  if [[ "$PURGE_QUAR" == "1" ]]; then purge_queue "$QUARANTINE_QUEUE_URL" "QUARANTINE"; fi
  if [[ "$PURGE_DLQ" == "1" ]]; then purge_queue "$DLQ_URL" "DLQ"; fi

  echo "==> Applying STRICT_THR=$STRICT_THR (EXPECT: QUARANTINE gets ~$COUNT msgs; MAIN stays 0; processedTotal ~0)"
  jq_merge_thr "$RISK_GATE_FN" "$STRICT_THR"

  # Also set on Generator/Processor just to eliminate config drift in your stacks (harmless)
  if [[ -n "${GENERATOR_FN:-}" ]]; then jq_merge_thr "$GENERATOR_FN" "$STRICT_THR"; fi
  if [[ -n "${PROCESSOR_FN:-}" ]]; then jq_merge_thr "$PROCESSOR_FN" "$STRICT_THR"; fi

  echo "==> Run guarded sanity (no CloudWatch needed)"
  MODE="guarded" "$RUN_ONE" "sanity_gate_strict" "$PROFILE" "$COUNT" 25

  echo "==> Sleep ${OBS_SLEEP}s to let gate enqueue"
  sleep "$OBS_SLEEP"

  local main_after quar_after dlq_after
  main_after="$(q_count "$MAIN_QUEUE_URL")"
  quar_after="$(q_count "$QUARANTINE_QUEUE_URL")"
  dlq_after="$(q_count "$DLQ_URL")"

  echo "==> MAIN before: $main_before"
  echo "==> MAIN after : $main_after"
  echo "==> QUAR before: $quar_before"
  echo "==> QUAR after : $quar_after"
  echo "==> DLQ  before: $dlq_before"
  echo "==> DLQ  after : $dlq_after"

  echo "==> Restoring DEFAULT_THR=$DEFAULT_THR"
  jq_merge_thr "$RISK_GATE_FN" "$DEFAULT_THR"
  if [[ -n "${GENERATOR_FN:-}" ]]; then jq_merge_thr "$GENERATOR_FN" "$DEFAULT_THR"; fi
  if [[ -n "${PROCESSOR_FN:-}" ]]; then jq_merge_thr "$PROCESSOR_FN" "$DEFAULT_THR"; fi

  cat <<'EOF'

INTERPRETATION (1 minute):
- If gate IS in-path:
  QUARANTINE ApproximateNumberOfMessages should jump by ~COUNT (or at least noticeably),
  MAIN should stay ~0, and your run artifact should show processedTotal ~0.
- If gate is NOT in-path:
  QUARANTINE stays ~same, MAIN gets messages, processedTotal grows (fanout => big amplification).

EOF
}

main "$@"