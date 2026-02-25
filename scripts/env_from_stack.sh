#!/usr/bin/env bash
set -euo pipefail

STACK="${1:?usage: env_from_stack.sh <stack-name> [out-file]}"
OUT_FILE="${2:-.env.$STACK}"

get_out() {
  local key="$1"
  aws cloudformation describe-stacks \
    --stack-name "$STACK" \
    --query "Stacks[0].Outputs[?OutputKey=='$key'].OutputValue" \
    --output text
}

cat > "$OUT_FILE" <<EOF
GENERATOR_FN=$(get_out GeneratorFunctionName)
PROCESSOR_FN=$(get_out ProcessorFunctionName)
RISK_GATE_FN=$(get_out RiskGateFunctionName)

MAIN_QUEUE_URL=$(get_out MainQueueUrl)
DLQ_URL=$(get_out DLQUrl)
QUARANTINE_QUEUE_URL=$(get_out QuarantineQueueUrl)

STATE_TABLE_NAME=$(get_out StateTableName)
EOF

echo "Wrote $OUT_FILE"
