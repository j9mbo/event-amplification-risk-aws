#!/usr/bin/env bash
set -euo pipefail

# scripts/run_ab_all.sh
# Minimal, predictable orchestrator:
# - Purges ONLY the MAIN queue (optional) to avoid 60s per-queue cooldown explosion.
# - Runs baseline + guarded repeats per profile.
# - Delegates ALL measurement to scripts/run_one.sh (which does NOT purge).

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ENV_BASELINE="${ENV_BASELINE:-.env.baseline}"
ENV_GUARDED="${ENV_GUARDED:-.env.guarded}"

# repeats
REPEATS_NORMAL="${REPEATS_NORMAL:-1}"
REPEATS_FANOUT="${REPEATS_FANOUT:-1}"
REPEATS_LOOP="${REPEATS_LOOP:-1}"
REPEATS_POISON="${REPEATS_POISON:-1}"

# purge controls
PURGE_MAIN="${PURGE_MAIN:-1}"                     # 1=yes, 0=no
PURGE_SLEEP_SECONDS="${PURGE_SLEEP_SECONDS:-70}"  # SQS purge cooldown safety
BETWEEN_RUN_SLEEP_SECONDS="${BETWEEN_RUN_SLEEP_SECONDS:-2}"

# run sizes (override if needed)
COUNT_NORMAL="${COUNT_NORMAL:-200}"
COUNT_POISON="${COUNT_POISON:-200}"
COUNT_FANOUT="${COUNT_FANOUT:-50}"
COUNT_LOOP="${COUNT_LOOP:-50}"

WAIT_NORMAL="${WAIT_NORMAL:-25}"
WAIT_FANOUT="${WAIT_FANOUT:-30}"
WAIT_LOOP="${WAIT_LOOP:-30}"
WAIT_POISON="${WAIT_POISON:-360}"

# quiescence knobs forwarded to run_one.sh
export REQUIRED_ZERO_STREAK="${REQUIRED_ZERO_STREAK:-3}"
export OBS_WINDOW_SECONDS="${OBS_WINDOW_SECONDS:-120}"
export DRAIN_CONFIRM_SLEEP="${DRAIN_CONFIRM_SLEEP:-2}"

use_env() {
  local env_file="$1" mode="$2"
  if [[ ! -f "$env_file" ]]; then
    echo "ERROR: env file not found: $env_file" >&2
    exit 2
  fi
  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a

  export MODE="$mode"
  export AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"

  echo "==> Using env=$env_file MODE=$MODE AWS_REGION=$AWS_REGION"
  echo "    GENERATOR_FN=$GENERATOR_FN"
  echo "    PROCESSOR_FN=$PROCESSOR_FN"
  echo "    RISK_GATE_FN=${RISK_GATE_FN:-}"
  echo "    MAIN_QUEUE_URL=$MAIN_QUEUE_URL"
}

purge_main_queue_once() {
  [[ "$PURGE_MAIN" -eq 1 ]] || return 0
  echo "==> Purging MAIN queue + cooldown ${PURGE_SLEEP_SECONDS}s"
  if ! aws sqs purge-queue --queue-url "$MAIN_QUEUE_URL" >/dev/null 2>&1; then
    echo "    PurgeQueueInProgress -> cooldown ${PURGE_SLEEP_SECONDS}s (no retry)"
  fi
  sleep "$PURGE_SLEEP_SECONDS"
}

run_case_repeated() {
  local prefix="$1" profile="$2" count="$3" wait="$4" reps="$5"
  for ((r=1; r<=reps; r++)); do
    ts="$(date -u +%Y%m%dT%H%M%SZ)"
    run_id="${prefix}_${ts}_r${r}"

    purge_main_queue_once

    echo "==> RUN: $run_id profile=$profile count=$count wait=$wait MODE=$MODE"
    ./scripts/run_one.sh "$run_id" "$profile" "$count" "$wait"

    sleep "$BETWEEN_RUN_SLEEP_SECONDS"
  done
}

#################################
# Baseline
#################################
use_env "$ENV_BASELINE" "baseline"
run_case_repeated "exp01_normal_baseline"  "normal" "$COUNT_NORMAL" "$WAIT_NORMAL" "$REPEATS_NORMAL"
run_case_repeated "exp02_poison_baseline"  "poison" "$COUNT_POISON" "$WAIT_POISON" "$REPEATS_POISON"
run_case_repeated "exp03_fanout_baseline"  "fanout" "$COUNT_FANOUT" "$WAIT_FANOUT" "$REPEATS_FANOUT"
run_case_repeated "exp04_loop_baseline"    "loop"   "$COUNT_LOOP"   "$WAIT_LOOP"   "$REPEATS_LOOP"

#################################
# Guarded
#################################
use_env "$ENV_GUARDED" "guarded"
run_case_repeated "exp11_normal_guarded"   "normal" "$COUNT_NORMAL" "$WAIT_NORMAL" "$REPEATS_NORMAL"
run_case_repeated "exp12_poison_guarded"   "poison" "$COUNT_POISON" "$WAIT_POISON" "$REPEATS_POISON"
run_case_repeated "exp13_fanout_guarded"   "fanout" "$COUNT_FANOUT" "$WAIT_FANOUT" "$REPEATS_FANOUT"
run_case_repeated "exp14_loop_guarded"     "loop"   "$COUNT_LOOP"   "$WAIT_LOOP"   "$REPEATS_LOOP"

echo "DONE. Artifacts: runs/*.json"