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
sam delete --stack-name event-amplification-risk-baseline --region us-east-1

sam delete --stack-name event-amplification-risk-guarded --region us-east-1
```
## Run generator
Invoke Generator Lambda with payload:
```
{ "count": 200, "profile": "mixed" }
```
## Experiment outputs
See `docs/experiment_protocol.md`.



## Testing 
### 1st Normal
```
aws lambda invoke \
  --function-name "$GENERATOR_FN" \
  --cli-binary-format raw-in-base64-out \
  --payload '{"count": 5, "profile": "normal"}' \
  out.json && cat out.json
```
```
aws sqs get-queue-attributes \
  --queue-url "$MAIN_QUEUE_URL" \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible

aws sqs get-queue-attributes \
  --queue-url "$DLQ_URL" \
  --attribute-names ApproximateNumberOfMessages

aws sqs get-queue-attributes \
  --queue-url "$QUARANTINE_QUEUE_URL" \
  --attribute-names ApproximateNumberOfMessages
```
```
aws logs tail "/aws/lambda/$PROCESSOR_FN" --since 2m
```
### 2nd Poison
```
aws lambda invoke \
  --function-name "$GENERATOR_FN" \
  --cli-binary-format raw-in-base64-out \
  --payload '{"count": 5, "profile": "poison"}' \
  out.json && cat out.json

```
```
aws sqs get-queue-attributes \
  --queue-url "$MAIN_QUEUE_URL" \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible

aws sqs get-queue-attributes \
  --queue-url "$DLQ_URL" \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible

aws sqs get-queue-attributes \
  --queue-url "$QUARANTINE_QUEUE_URL" \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible
```
```
aws sqs receive-message \
  --queue-url "$DLQ_URL" \
  --max-number-of-messages 5 \
  --wait-time-seconds 1
```
```
aws logs tail "/aws/lambda/$PROCESSOR_FN" --since 5m
```

### 3rd FanOut
```
aws lambda invoke \
  --function-name "$GENERATOR_FN" \
  --cli-binary-format raw-in-base64-out \
  --payload '{"count": 2, "profile": "fanout"}' \
  out.json && cat out.json

```
```
aws sqs get-queue-attributes \
  --queue-url "$MAIN_QUEUE_URL" \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible

```
```
aws logs tail "/aws/lambda/$PROCESSOR_FN" --since 5m

```
### 4th LOOP
```
aws lambda invoke \
  --function-name "$GENERATOR_FN" \
  --cli-binary-format raw-in-base64-out \
  --payload '{"count": 2, "profile": "loop"}' \
  out.json && cat out.json
```
```
aws sqs get-queue-attributes \
  --queue-url "$MAIN_QUEUE_URL" \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible
```

## Real Scenarios - automated
#### Clear DLQ:
```
aws sqs purge-queue --queue-url "$DLQ_URL"
```
#### Clear Quarantine quees (WHen using Guarded):
```
aws sqs purge-queue --queue-url "$QUARANTINE_QUEUE_URL"
```
### Scenarion A - Baseline -> Main queue
```
aws sqs purge-queue --queue-url "$MAIN_QUEUE_URL"; aws sqs purge-queue --queue-url "$DLQ_URL"; aws sqs purge-queue --queue-url "$QUARANTINE_QUEUE_URL"; sleep 70
./scripts/run_one.sh exp01_normal_baseline normal 200 25

aws sqs purge-queue --queue-url "$MAIN_QUEUE_URL"; aws sqs purge-queue --queue-url "$DLQ_URL"; aws sqs purge-queue --queue-url "$QUARANTINE_QUEUE_URL"; sleep 70
./scripts/run_one.sh exp02_poison_baseline poison 200 120

aws sqs purge-queue --queue-url "$MAIN_QUEUE_URL"; aws sqs purge-queue --queue-url "$DLQ_URL"; aws sqs purge-queue --queue-url "$QUARANTINE_QUEUE_URL"; sleep 70
./scripts/run_one.sh exp03_fanout_baseline fanout 50 30

aws sqs purge-queue --queue-url "$MAIN_QUEUE_URL"; aws sqs purge-queue --queue-url "$DLQ_URL"; aws sqs purge-queue --queue-url "$QUARANTINE_QUEUE_URL"; sleep 70
./scripts/run_one.sh exp04_loop_baseline loop 50 30
```
### Scenario B - Risk Gate
```
aws sqs purge-queue --queue-url "$MAIN_QUEUE_URL"; aws sqs purge-queue --queue-url "$DLQ_URL"; aws sqs purge-queue --queue-url "$QUARANTINE_QUEUE_URL"; sleep 70
./scripts/run_one.sh exp11_normal_guarded normal 200 25

aws sqs purge-queue --queue-url "$MAIN_QUEUE_URL"; aws sqs purge-queue --queue-url "$DLQ_URL"; aws sqs purge-queue --queue-url "$QUARANTINE_QUEUE_URL"; sleep 70
./scripts/run_one.sh exp12_poison_guarded poison 200 25

aws sqs purge-queue --queue-url "$MAIN_QUEUE_URL"; aws sqs purge-queue --queue-url "$DLQ_URL"; aws sqs purge-queue --queue-url "$QUARANTINE_QUEUE_URL"; sleep 70
./scripts/run_one.sh exp13_fanout_guarded fanout 50 30

aws sqs purge-queue --queue-url "$MAIN_QUEUE_URL"; aws sqs purge-queue --queue-url "$DLQ_URL"; aws sqs purge-queue --queue-url "$QUARANTINE_QUEUE_URL"; sleep 70./scripts/run_one.sh exp14_loop_guarded loop 50 30
```




### new testing:
```
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

```
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