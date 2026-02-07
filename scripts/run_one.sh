#!/usr/bin/env bash
set -euo pipefail

# ---- prerequisites ----
command -v aws >/dev/null || { echo "aws cli is required"; exit 1; }
command -v jq  >/dev/null || { echo "jq is required"; exit 1; }

# ---- load env ----
if [[ -f ".env" ]]; then
  set -a
  source .env
  set +a
fi

: "${GENERATOR_FN:?GENERATOR_FN is required}"
: "${STATE_TABLE_NAME:?STATE_TABLE_NAME is required}"
: "${MAIN_QUEUE_URL:?MAIN_QUEUE_URL is required}"
: "${DLQ_URL:?DLQ_URL is required}"
: "${QUARANTINE_QUEUE_URL:?QUARANTINE_QUEUE_URL is required}"

RUN_ID="${1:-}"
PROFILE="${2:-normal}"
COUNT="${3:-100}"
WAIT_SEC="${4:-20}"

if [[ -z "$RUN_ID" ]]; then
  echo "Usage: $0 <runId> [profile] [count] [waitSeconds]"
  echo "Example: $0 exp01_normal_baseline normal 200 20"
  exit 1
fi

mkdir -p runs

echo "==> Trigger generator: runId=$RUN_ID profile=$PROFILE count=$COUNT"
aws lambda invoke \
  --function-name "$GENERATOR_FN" \
  --cli-binary-format raw-in-base64-out \
  --payload "{\"runId\":\"$RUN_ID\",\"count\":$COUNT,\"profile\":\"$PROFILE\"}" \
  "runs/${RUN_ID}.invoke.json" >/dev/null

echo "==> Generator response:"
cat "runs/${RUN_ID}.invoke.json" | jq .

# ---- helper: queue attributes ----
get_q_attrs() {
  local url="$1"
  aws sqs get-queue-attributes \
    --queue-url "$url" \
    --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible \
  | jq -c '.Attributes'
}

# ---- wait until processor catches up (simple stabilization loop) ----
echo "==> Waiting for stabilization (up to ${WAIT_SEC}s)..."
end_ts=$(( $(date +%s) + WAIT_SEC ))
prev_count="-1"

while [[ $(date +%s) -lt $end_ts ]]; do
  # DynamoDB count for this run (scan + filter; OK for lab size)
  cur_count=$(
    aws dynamodb scan \
      --table-name "$STATE_TABLE_NAME" \
      --select "COUNT" \
      --filter-expression "correlationId = :c" \
      --expression-attribute-values "{\":c\":{\"S\":\"$RUN_ID\"}}" \
    | jq -r '.Count'
  )

  # break if count stops changing and main queue looks empty
  main_attrs="$(get_q_attrs "$MAIN_QUEUE_URL")"
  main_visible="$(echo "$main_attrs" | jq -r '.ApproximateNumberOfMessages // "0"')"
  main_notvis="$(echo "$main_attrs" | jq -r '.ApproximateNumberOfMessagesNotVisible // "0"')"

  if [[ "$cur_count" == "$prev_count" && "$main_visible" == "0" && "$main_notvis" == "0" ]]; then
    break
  fi

  prev_count="$cur_count"
  sleep 2
done

echo "==> Collecting results..."

# ---- DynamoDB: get items for run (projection for speed) ----
ddb_json=$(
  aws dynamodb scan \
    --table-name "$STATE_TABLE_NAME" \
    --filter-expression "correlationId = :c" \
    --expression-attribute-values "{\":c\":{\"S\":\"$RUN_ID\"}}" \
    --projection-expression "correlationId,eventType,eventId,hopCount,fanoutDegree,approxReceiveCount,outcome,ts"
)

# ---- Aggregations from DynamoDB scan ----
processed_total="$(echo "$ddb_json" | jq -r '.Count')"

by_type="$(echo "$ddb_json" | jq -c '
  [.Items[].eventType.S] | group_by(.) | map({eventType: .[0], count: length})
')"

by_outcome="$(echo "$ddb_json" | jq -c '
  [.Items[].outcome.S] | group_by(.) | map({outcome: .[0], count: length})
')"

max_hops="$(echo "$ddb_json" | jq -r '
  ([.Items[].hopCount.N|tonumber] | max) // 0
')"

max_fanout="$(echo "$ddb_json" | jq -r '
  ([.Items[].fanoutDegree.N|tonumber] | max) // 0
')"

avg_receive="$(echo "$ddb_json" | jq -r '
  ( [.Items[].approxReceiveCount.N|tonumber] | (add / length) ) // 0
')"

# ---- queue snapshots ----
main_attrs="$(get_q_attrs "$MAIN_QUEUE_URL")"
dlq_attrs="$(get_q_attrs "$DLQ_URL")"
qu_attrs="$(get_q_attrs "$QUARANTINE_QUEUE_URL")"

# ---- amplification factor ----
# input_events = COUNT (sent by generator)
# processed_events = processed_total
amplification="$(jq -n --argjson p "$processed_total" --argjson i "$COUNT" '
  if $i == 0 then 0 else ($p / $i) end
')"

# ---- final report ----
report="$(jq -n \
  --arg runId "$RUN_ID" \
  --arg profile "$PROFILE" \
  --arg mode "$(cat "runs/${RUN_ID}.invoke.json" | jq -r '.mode // "unknown"')" \
  --argjson inputEvents "$COUNT" \
  --argjson processedEvents "$processed_total" \
  --argjson amplificationFactor "$amplification" \
  --argjson maxHopCount "$max_hops" \
  --argjson maxFanoutDegree "$max_fanout" \
  --argjson avgApproxReceiveCount "$avg_receive" \
  --argjson byEventType "$by_type" \
  --argjson byOutcome "$by_outcome" \
  --argjson mainQueue "$main_attrs" \
  --argjson deadLetterQueue "$dlq_attrs" \
  --argjson quarantineQueue "$qu_attrs" \
  '{
    runId: $runId,
    profile: $profile,
    mode: $mode,
    inputEvents: $inputEvents,
    processedEvents: $processedEvents,
    amplificationFactor: $amplificationFactor,
    maxHopCount: $maxHopCount,
    maxFanoutDegree: $maxFanoutDegree,
    avgApproxReceiveCount: $avgApproxReceiveCount,
    byEventType: $byEventType,
    byOutcome: $byOutcome,
    queues: {
      main: $mainQueue,
      deadLetter: $deadLetterQueue,
      quarantine: $quarantineQueue
    }
  }'
)"

echo "$report" | jq . | tee "runs/${RUN_ID}.json" >/dev/null

echo "==> Saved: runs/${RUN_ID}.json"
echo "==> Summary: processed=$processed_total input=$COUNT amplification=$(echo "$amplification" | jq -r '.')"
