# event-amplification-risk-aws

Experimental AWS serverless testbed for predicting and containing event amplification (storms/loops) in event-driven pipelines.

## Modes
- baseline: SQS -> Processor Lambda with static retries + DLQ
- guarded: Risk Gate (risk scoring) routes events to Main vs Quarantine

## Deploy (SAM)
From `infra/sam/`:

```
sam build
sam deploy --guided
```

## Run generator
Invoke Generator Lambda with payload:
```
{ "count": 200, "profile": "mixed" }
```
## Experiment outputs
See `docs/experiment_protocol.md`.
