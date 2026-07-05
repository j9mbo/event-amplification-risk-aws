#!/usr/bin/env bash
set -euo pipefail

# scripts/run_mask_ab.sh
# Campaign: honest fanout vs masking profiles (s1/s2/s3), in two containment configs --
# "gate only" (MAX_FANOUT=50) and "full defense" (MAX_FANOUT=5), at the Zone B threshold
# (RISK_THRESHOLD=0.25).
#
# Delegates the measurement to run_one.sh (which does NOT purge the queues).
# Safely merges one Lambda env var (like run_ab_all.sh) -- nothing else is overwritten.
#
# Params (via env):
#   PROFILES   (default "fanout s1 s2 s3")
#   MAXFANOUTS (default "50 5"  -> gateOnly, fullDefense)
#   REPEATS    (default 10; use 1 for a smoke run)

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ENV_GUARDED="${ENV_GUARDED:-.env.guarded}"
PROFILES="${PROFILES:-fanout s1 s2 s3}"
MAXFANOUTS="${MAXFANOUTS:-50 5}"
REPEATS="${REPEATS:-10}"
COUNT="${COUNT:-50}"
WAIT="${WAIT:-30}"

ZONE_B_THR="${ZONE_B_THR:-0.25}"
DEFAULT_THR="${DEFAULT_THR:-0.80}"
DEFAULT_MAXFANOUT="${DEFAULT_MAXFANOUT:-5}"

PURGE_SLEEP_SECONDS="${PURGE_SLEEP_SECONDS:-70}"
export OBS_WINDOW_SECONDS="${OBS_WINDOW_SECONDS:-120}"
export REQUIRED_ZERO_STREAK="${REQUIRED_ZERO_STREAK:-3}"

# --- env ---
[[ -f "$ENV_GUARDED" ]] || { echo "ERROR: no $ENV_GUARDED" >&2; exit 2; }
set -a; source "$ENV_GUARDED"; set +a
export ENV_FILE="$ENV_GUARDED"
export MODE="guarded"
export AWS_REGION="${AWS_REGION:-us-east-1}"

# Safe merge of a single Lambda env var (keeps the rest).
merge_env() {
  local fn="$1" key="$2" val="$3" vars env
  vars="$(aws lambda get-function-configuration --function-name "$fn" \
           --query 'Environment.Variables' --output json)"
  env="$(jq -c --arg k "$key" --arg v "$val" '.[$k]=$v | {Variables: .}' <<<"$vars")"
  aws lambda update-function-configuration --function-name "$fn" --environment "$env" >/dev/null
  aws lambda wait function-updated --function-name "$fn"
}

purge_all() {
  aws sqs purge-queue --queue-url "$MAIN_QUEUE_URL" >/dev/null 2>&1 || true
  aws sqs purge-queue --queue-url "$QUARANTINE_QUEUE_URL" >/dev/null 2>&1 || true
  aws sqs purge-queue --queue-url "$DLQ_URL" >/dev/null 2>&1 || true
  sleep "$PURGE_SLEEP_SECONDS"
}

echo "==> Gate threshold -> Zone B ($ZONE_B_THR)"
merge_env "$RISK_GATE_FN" "RISK_THRESHOLD" "$ZONE_B_THR"

for mf in $MAXFANOUTS; do
  if [[ "$mf" == "50" ]]; then cfg="gateOnly"; else cfg="fullDef"; fi
  echo "==> Config $cfg: MAX_FANOUT=$mf"
  merge_env "$PROCESSOR_FN" "MAX_FANOUT" "$mf"
  for profile in $PROFILES; do
    for ((r=1; r<=REPEATS; r++)); do
      ts="$(date -u +%Y%m%dT%H%M%SZ)"
      # Unique seed per repeat -- same across profiles/configs within repeat r (matched
      # A/B pairs), different across repeats (variance + ML data).
      export RUN_SEED=$(( ${SEED_BASE:-1000} + r ))
      run_id="maskB_${profile}_${cfg}_guarded_${ts}_r${r}_seed${RUN_SEED}"
      echo "==> RUN $run_id"
      purge_all
      ./scripts/run_one.sh "$run_id" "$profile" "$COUNT" "$WAIT"
    done
  done
done

echo "==> Restoring defaults: RISK_THRESHOLD=$DEFAULT_THR, MAX_FANOUT=$DEFAULT_MAXFANOUT"
merge_env "$RISK_GATE_FN" "RISK_THRESHOLD" "$DEFAULT_THR"
merge_env "$PROCESSOR_FN" "MAX_FANOUT" "$DEFAULT_MAXFANOUT"
echo "DONE. Artifacts: runs/*.json"
