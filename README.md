# event-amplification-risk-aws

Experimental AWS serverless testbed for predicting and containing event amplification (storms/loops) in event-driven pipelines.

## Modes
- baseline: SQS -> Processor Lambda with static retries + DLQ
- guarded: Risk Gate (risk scoring) routes events to Main vs Quarantine

## Deploy (SAM)
From `infra/sam/`:

```
## Baseline
sam build
sam deploy \
  --stack-name event-amplification-risk-baseline \
  --resolve-s3 \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides Mode=baseline RiskThreshold="0.80" \
  --no-confirm-changeset

## Guarded
sam build
sam deploy \
  --stack-name event-amplification-risk-guarded \
  --resolve-s3 \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides Mode=guarded RiskThreshold="0.80" \
  --no-confirm-changeset

```
## Destroy/delete (SAM)
From `infra/sam/`:

```
sam delete --stack-name event-amplification-risk-baseline --region us-east-1 --no-prompts

sam delete --stack-name event-amplification-risk-guarded --region us-east-1 --no-prompts
```
## Run generator
Invoke Generator Lambda with payload:
```
{ "count": 200, "profile": "mixed" }
```
## Experiment outputs
See `docs/experiment_protocol.md`.



## Testing 
## Real Scenarios - automated
### Baseline
```
./scripts/env_from_stack.sh event-amplification-risk-baseline .env.baseline
cp .env.baseline .env
source .env
```
```
# A1: normal
aws sqs purge-queue --queue-url "$MAIN_QUEUE_URL"; aws sqs purge-queue --queue-url "$DLQ_URL"; aws sqs purge-queue --queue-url "$QUARANTINE_QUEUE_URL"; sleep 70
./scripts/run_one.sh exp01_normal_baseline normal 200 25

# A2: poison
aws sqs purge-queue --queue-url "$MAIN_QUEUE_URL"; aws sqs purge-queue --queue-url "$DLQ_URL"; aws sqs purge-queue --queue-url "$QUARANTINE_QUEUE_URL"; sleep 70
./scripts/run_one.sh exp02_poison_baseline poison 200 360

# A3: fanout
aws sqs purge-queue --queue-url "$MAIN_QUEUE_URL"; aws sqs purge-queue --queue-url "$DLQ_URL"; aws sqs purge-queue --queue-url "$QUARANTINE_QUEUE_URL"; sleep 70
./scripts/run_one.sh exp03_fanout_baseline fanout 50 30

# A4: loop
aws sqs purge-queue --queue-url "$MAIN_QUEUE_URL"; aws sqs purge-queue --queue-url "$DLQ_URL"; aws sqs purge-queue --queue-url "$QUARANTINE_QUEUE_URL"; sleep 70
./scripts/run_one.sh exp04_loop_baseline loop 50 30
```
### Guarded
```
./scripts/env_from_stack.sh event-amplification-risk-guarded .env.guarded
cp .env.guarded .env
source .env
```
```
# B1: normal
aws sqs purge-queue --queue-url "$MAIN_QUEUE_URL"; aws sqs purge-queue --queue-url "$DLQ_URL"; aws sqs purge-queue --queue-url "$QUARANTINE_QUEUE_URL"; sleep 70
./scripts/run_one.sh exp11_normal_guarded normal 200 25

# B2: poison (краще більший max_wait)
aws sqs purge-queue --queue-url "$MAIN_QUEUE_URL"; aws sqs purge-queue --queue-url "$DLQ_URL"; aws sqs purge-queue --queue-url "$QUARANTINE_QUEUE_URL"; sleep 70
./scripts/run_one.sh exp12_poison_guarded poison 200 360

# B3: fanout
aws sqs purge-queue --queue-url "$MAIN_QUEUE_URL"; aws sqs purge-queue --queue-url "$DLQ_URL"; aws sqs purge-queue --queue-url "$QUARANTINE_QUEUE_URL"; sleep 70
./scripts/run_one.sh exp13_fanout_guarded fanout 50 30

# B4: loop
aws sqs purge-queue --queue-url "$MAIN_QUEUE_URL"; aws sqs purge-queue --queue-url "$DLQ_URL"; aws sqs purge-queue --queue-url "$QUARANTINE_QUEUE_URL"; sleep 70
./scripts/run_one.sh exp14_loop_guarded loop 50 30
```


```
REPEATS_NORMAL=1 REPEATS_LOOP=1 REPEATS_FANOUT=3 REPEATS_POISON=3 \
PURGE_SLEEP_SECONDS=70 BETWEEN_RUN_SLEEP_SECONDS=2 \
./scripts/run_ab_all.sh
```
