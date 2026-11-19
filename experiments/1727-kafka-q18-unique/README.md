# Q18 Benchmark

This experiment compares Justin and DS2 on the state-heavy `q18_unique` Flink
SQL query.

## Configuration

| Setting | Value |
|---|---|
| Producer rate | 97,827 mixed Nexmark events/s |
| Approximate bid rate | 90K events/s |
| Generated events | 100M |
| TaskManager resources | 8 CPU, 3 GiB memory, 8 slots |
| Managed memory | 1264 MiB per TaskManager |
| Metrics window | 2 minutes |
| Stabilization interval | 3 minutes |
| Pipeline maximum parallelism | 360 |
| Autoscaler vertex maximum | 12 |

Initial job parallelism:

| Operator | Parallelism | Autoscaled |
|---|---:|:---:|
| Bid Kafka source | 3 | no |
| Deduplicate and sink | 1 | yes |

Each policy has a complete standalone manifest:

- `jobs/justin/experiment.yaml`
- `jobs/ds2/experiment.yaml`
