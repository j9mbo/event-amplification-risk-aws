#!/usr/bin/env bash
set -euo pipefail

# scripts/run_one.sh
# Runs a single experiment and writes runs/<runId>.json.
# NOTE: Purging queues is intentionally NOT done here (to avoid double-purge + 60s cooldown issues).
#       Do purge in a higher-level orchestrator (e.g., run_ab_all.sh).

# load .env (if exists)
maybe_source_env() {
  local f="$1"
  [[ -f "$f" ]] || return 0
  set -a
  source "$f"
  set +a
}

if [[ -n "$ENV_FILE" ]]; then
  maybe_source_env "$ENV_FILE"
elif [[ -f ".env" ]]; then
  if [[ -z "${GENERATOR_FN:-}" || -z "${PROCESSOR_FN:-}" || -z "${MAIN_QUEUE_URL:-}" || -z "${DLQ_URL:-}" || -z "${QUARANTINE_QUEUE_URL:-}" || -z "${STATE_TABLE_NAME:-}" ]]; then
    maybe_source_env ".env"
  fi
fi

: "${GENERATOR_FN:?GENERATOR_FN is required}"
: "${PROCESSOR_FN:?PROCESSOR_FN is required}"
: "${MAIN_QUEUE_URL:?MAIN_QUEUE_URL is required}"
: "${DLQ_URL:?DLQ_URL is required}"
: "${QUARANTINE_QUEUE_URL:?QUARANTINE_QUEUE_URL is required}"
: "${STATE_TABLE_NAME:?STATE_TABLE_NAME is required}"

RUN_ID="${1:?runId}"
PROFILE="${2:?profile}"
COUNT="${3:?count}"
MAX_WAIT="${4:-30}"

# Optional MODE override; otherwise derive from runId.
if [[ -z "${MODE:-}" ]]; then
  if [[ "$RUN_ID" == *"_guarded"* || "$RUN_ID" == *"_gateonly"* ]]; then
    MODE="guarded"
  else
    MODE="baseline"
  fi
fi

# fail-fast: when guarded, require RISK_GATE_FN (optional but consistent)
if [[ "$MODE" == "guarded" && -z "${RISK_GATE_FN:-}" ]]; then
  echo "ERROR: MODE=guarded but RISK_GATE_FN is not set"
  exit 2
fi

# Tunables (override via env)
REQUIRED_ZERO_STREAK="${REQUIRED_ZERO_STREAK:-3}"   # consecutive 0/0 observations
OBS_WINDOW_SECONDS="${OBS_WINDOW_SECONDS:-120}"     # post-wait stabilization window
DRAIN_CONFIRM_SLEEP="${DRAIN_CONFIRM_SLEEP:-2}"     # extra confirm delay after streak reached

# CloudWatch lag buffers (optional; CW can be 0 due to lag)
CW_LAG_START_BUFFER="${CW_LAG_START_BUFFER:-120}"
CW_LAG_END_BUFFER="${CW_LAG_END_BUFFER:-300}"
CW_PERIOD_SECONDS="${CW_PERIOD_SECONDS:-60}"

now_epoch() { date +%s; }
to_iso() { date -u -r "$1" +"%Y-%m-%dT%H:%M:%SZ"; }

sqs_attrs() {
  local url="$1"
  aws sqs get-queue-attributes \
    --queue-url "$url" \
    --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
  | jq -c '.Attributes | {visible:(.ApproximateNumberOfMessages|tonumber), notVisible:(.ApproximateNumberOfMessagesNotVisible|tonumber)}'
}

cloudwatch_sum() {
  local fn="$1" metric="$2" start_iso="$3" end_iso="$4"
  aws cloudwatch get-metric-statistics \
    --namespace AWS/Lambda \
    --metric-name "$metric" \
    --dimensions Name=FunctionName,Value="$fn" \
    --start-time "$start_iso" \
    --end-time "$end_iso" \
    --period "$CW_PERIOD_SECONDS" \
    --statistics Sum \
  | jq '[.Datapoints[].Sum] | add // 0'
}

cloudwatch_avg_duration_ms() {
  local fn="$1" start_iso="$2" end_iso="$3"
  aws cloudwatch get-metric-statistics \
    --namespace AWS/Lambda \
    --metric-name Duration \
    --dimensions Name=FunctionName,Value="$fn" \
    --start-time "$start_iso" \
    --end-time "$end_iso" \
    --period "$CW_PERIOD_SECONDS" \
    --statistics Average \
  | jq '[.Datapoints[].Average] | if length == 0 then 0 else (add / length) end'
}

# Snapshots BEFORE
MAIN_BEFORE="$(sqs_attrs "$MAIN_QUEUE_URL")"
DLQ_BEFORE="$(sqs_attrs "$DLQ_URL")"
QUAR_BEFORE="$(sqs_attrs "$QUARANTINE_QUEUE_URL")"

START_TS="$(now_epoch)"
START_ISO="$(to_iso "$START_TS")"

echo "==> Trigger generator: runId=$RUN_ID profile=$PROFILE count=$COUNT MODE=$MODE"

TMP_PAYLOAD_FILE="$(mktemp)"
cleanup() { rm -f "$TMP_PAYLOAD_FILE"; }
trap cleanup EXIT

INVOKE_META="$(aws lambda invoke \
  --function-name "$GENERATOR_FN" \
  --cli-binary-format raw-in-base64-out \
  --payload "{\"count\": $COUNT, \"profile\": \"$PROFILE\", \"runId\": \"$RUN_ID\"}" \
  "$TMP_PAYLOAD_FILE"
)"

GEN_OUT="$(cat "$TMP_PAYLOAD_FILE")"
INPUT_SENT="$(echo "$GEN_OUT" | jq -r '.sent // 0')"

# Wait for quiescence
DRAINED=0
ZERO_STREAK=0
QUIESCENT_TS=""; QUIESCENT_ISO=""; TIME_TO_QUIESCENCE=""
WAIT_CAPPED_SECONDS=0
WAIT_OUTCOME="capped"

for ((i=0;i<MAX_WAIT;i++)); do
  A="$(sqs_attrs "$MAIN_QUEUE_URL")"
  V="$(echo "$A" | jq -r '.visible')"
  NV="$(echo "$A" | jq -r '.notVisible')"

  if [[ "$V" -eq 0 && "$NV" -eq 0 ]]; then
    ZERO_STREAK=$((ZERO_STREAK+1))
    if [[ "$ZERO_STREAK" -ge "$REQUIRED_ZERO_STREAK" ]]; then
      # Extra confirmation to reduce flakes from approximate attrs
      sleep "$DRAIN_CONFIRM_SLEEP"
      A2="$(sqs_attrs "$MAIN_QUEUE_URL")"
      V2="$(echo "$A2" | jq -r '.visible')"
      NV2="$(echo "$A2" | jq -r '.notVisible')"
      if [[ "$V2" -eq 0 && "$NV2" -eq 0 ]]; then
        DRAINED=1
        QUIESCENT_TS="$(now_epoch)"
        QUIESCENT_ISO="$(to_iso "$QUIESCENT_TS")"
        TIME_TO_QUIESCENCE="$((QUIESCENT_TS-START_TS))"
        WAIT_OUTCOME="quiescent"
        break
      else
        ZERO_STREAK=0
      fi
    fi
  else
    ZERO_STREAK=0
  fi
  sleep 1
done

if [[ "$DRAINED" -eq 0 ]]; then
  WAIT_CAPPED_SECONDS="$MAX_WAIT"
fi

# Stabilization / observation window
sleep "$OBS_WINDOW_SECONDS"

END_TS="$(now_epoch)"
END_ISO="$(to_iso "$END_TS")"

# Snapshots AFTER
MAIN_AFTER="$(sqs_attrs "$MAIN_QUEUE_URL")"
DLQ_AFTER="$(sqs_attrs "$DLQ_URL")"
QUAR_AFTER="$(sqs_attrs "$QUARANTINE_QUEUE_URL")"

MAIN_AFTER_V="$(echo "$MAIN_AFTER" | jq -r '.visible')"
MAIN_AFTER_NV="$(echo "$MAIN_AFTER" | jq -r '.notVisible')"
DRAIN_VERIFIED_POST=0
if [[ "$MAIN_AFTER_V" -eq 0 && "$MAIN_AFTER_NV" -eq 0 ]]; then
  DRAIN_VERIFIED_POST=1
fi

RUN_VALID=1
if [[ "$DRAINED" -eq 0 || "$DRAIN_VERIFIED_POST" -eq 0 ]]; then
  RUN_VALID=0
fi

DLQ_DELTA="$(jq -n --argjson b "$DLQ_BEFORE" --argjson a "$DLQ_AFTER" '($a.visible+$a.notVisible) - ($b.visible+$b.notVisible)')"
QUAR_DELTA="$(jq -n --argjson b "$QUAR_BEFORE" --argjson a "$QUAR_AFTER" '($a.visible+$a.notVisible) - ($b.visible+$b.notVisible)')"

# DynamoDB counts (main signal)
TOTAL="$(aws dynamodb scan --table-name "$STATE_TABLE_NAME" \
  --filter-expression "correlationId = :c" \
  --expression-attribute-values "{\":c\":{\"S\":\"$RUN_ID\"}}" \
  --projection-expression "outcome" \
| jq '.Items | length')"

SUCCESS="$(aws dynamodb scan --table-name "$STATE_TABLE_NAME" \
  --filter-expression "correlationId = :c" \
  --expression-attribute-values "{\":c\":{\"S\":\"$RUN_ID\"}}" \
  --projection-expression "outcome" \
| jq '[.Items[].outcome.S] | map(select(.=="success")) | length')"

FAIL="$((TOTAL-SUCCESS))"

AMPLIFICATION="$(jq -n --arg i "$INPUT_SENT" --arg p "$TOTAL" 'if ($i|tonumber)==0 then 0 else (($p|tonumber)/($i|tonumber)) end')"

# CloudWatch metrics (best-effort)
CW_START_TS=$((START_TS - CW_LAG_START_BUFFER))
CW_END_TS=$((END_TS + CW_LAG_END_BUFFER))
CW_START_ISO="$(to_iso "$CW_START_TS")"
CW_END_ISO="$(to_iso "$CW_END_TS")"

PROC_INV="$(cloudwatch_sum "$PROCESSOR_FN" "Invocations" "$CW_START_ISO" "$CW_END_ISO")"
PROC_ERR="$(cloudwatch_sum "$PROCESSOR_FN" "Errors" "$CW_START_ISO" "$CW_END_ISO")"
PROC_THR="$(cloudwatch_sum "$PROCESSOR_FN" "Throttles" "$CW_START_ISO" "$CW_END_ISO")"
PROC_DUR="$(cloudwatch_avg_duration_ms "$PROCESSOR_FN" "$CW_START_ISO" "$CW_END_ISO")"

RISK_INV=0; RISK_ERR=0; RISK_THR=0; RISK_DUR=0
if [[ "$MODE" == "guarded" && -n "${RISK_GATE_FN:-}" ]]; then
  RISK_INV="$(cloudwatch_sum "$RISK_GATE_FN" "Invocations" "$CW_START_ISO" "$CW_END_ISO")"
  RISK_ERR="$(cloudwatch_sum "$RISK_GATE_FN" "Errors" "$CW_START_ISO" "$CW_END_ISO")"
  RISK_THR="$(cloudwatch_sum "$RISK_GATE_FN" "Throttles" "$CW_START_ISO" "$CW_END_ISO")"
  RISK_DUR="$(cloudwatch_avg_duration_ms "$RISK_GATE_FN" "$CW_START_ISO" "$CW_END_ISO")"
fi

mkdir -p runs
OUT="runs/${RUN_ID}.json"

if [[ "$DRAINED" -eq 1 ]]; then
  QUIESCENT_JSON="$(jq -n --arg iso "$QUIESCENT_ISO" --argjson t "$TIME_TO_QUIESCENCE" '{quiescentIso:$iso, timeToQuiescenceSeconds:$t}')"
else
  QUIESCENT_JSON="$(jq -n '{quiescentIso:null, timeToQuiescenceSeconds:null}')"
fi

jq -n \
  --arg runId "$RUN_ID" \
  --arg profile "$PROFILE" \
  --arg mode "$MODE" \
  --argjson count "$COUNT" \
  --arg startIso "$START_ISO" \
  --arg endIso "$END_ISO" \
  --argjson drained "$DRAINED" \
  --argjson drainVerifiedPostSnapshot "$DRAIN_VERIFIED_POST" \
  --argjson runValid "$RUN_VALID" \
  --arg waitOutcome "$WAIT_OUTCOME" \
  --argjson waitCappedSeconds "$WAIT_CAPPED_SECONDS" \
  --argjson observationWindowSeconds "$OBS_WINDOW_SECONDS" \
  --arg cwStartIso "$CW_START_ISO" \
  --arg cwEndIso "$CW_END_ISO" \
  --argjson cwPeriodSeconds "$CW_PERIOD_SECONDS" \
  --argjson inputSent "$INPUT_SENT" \
  --argjson processedTotal "$TOTAL" \
  --argjson processedSuccess "$SUCCESS" \
  --argjson processedFail "$FAIL" \
  --argjson amplification "$AMPLIFICATION" \
  --argjson mainBefore "$MAIN_BEFORE" --argjson mainAfter "$MAIN_AFTER" \
  --argjson dlqBefore "$DLQ_BEFORE" --argjson dlqAfter "$DLQ_AFTER" \
  --argjson quarantineBefore "$QUAR_BEFORE" --argjson quarantineAfter "$QUAR_AFTER" \
  --argjson dlqDelta "$DLQ_DELTA" --argjson quarantineDelta "$QUAR_DELTA" \
  --argjson procInv "$PROC_INV" --argjson procErr "$PROC_ERR" --argjson procThr "$PROC_THR" --argjson procDur "$PROC_DUR" \
  --argjson riskInv "$RISK_INV" --argjson riskErr "$RISK_ERR" --argjson riskThr "$RISK_THR" --argjson riskDur "$RISK_DUR" \
  --argjson quiescence "$QUIESCENT_JSON" \
'{
  runId:$runId, profile:$profile, mode:$mode, count:$count,
  time:{
    startIso:$startIso,
    endIso:$endIso,
    drained:$drained,
    drainVerifiedPostSnapshot:$drainVerifiedPostSnapshot,
    waitOutcome:$waitOutcome,
    waitCappedSeconds:$waitCappedSeconds,
    observationWindowSeconds:$observationWindowSeconds,
    quiescence:$quiescence
  },
  io:{
    inputSent:$inputSent,
    processedTotal:$processedTotal,
    processedSuccess:$processedSuccess,
    processedFail:$processedFail,
    amplification:$amplification
  },
  queues:{
    mainBefore:$mainBefore, mainAfter:$mainAfter,
    dlqBefore:$dlqBefore, dlqAfter:$dlqAfter, dlqDelta:$dlqDelta,
    quarantineBefore:$quarantineBefore, quarantineAfter:$quarantineAfter, quarantineDelta:$quarantineDelta
  },
  cloudwatch:{
    window:{startIso:$cwStartIso, endIso:$cwEndIso, periodSeconds:$cwPeriodSeconds},
    processor:{invocations:$procInv, errors:$procErr, throttles:$procThr, avgDurationMs:$procDur},
    riskGate:{invocations:$riskInv, errors:$riskErr, throttles:$riskThr, avgDurationMs:$riskDur}
  }
}' > "$OUT"

echo "==> Saved: $OUT"