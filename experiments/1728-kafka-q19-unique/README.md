# Q19 Benchmark

This experiment compares Justin and DS2 on the state-heavy `q19_unique` Flink
SQL query.

## Configuration

| Setting | Value |
|---|---|
| Producer rate | 59,783 mixed Nexmark events/s |
| Approximate bid rate | 55K events/s |
| Generated events | 100M |
| TaskManager resources | 8 CPU, 3 GiB memory, 8 slots |
| Managed memory | 1264 MiB per TaskManager |
| Metrics window | 2 minutes |
| Stabilization interval | 3 minutes |
| Maximum parallelism | 18 |

Initial job parallelism:

| Operator | Parallelism | Autoscaled |
|---|---:|:---:|
| Bid Kafka source | 1 | no |
| Rank and sink | 1 | yes |

Each policy has a complete standalone manifest:

- `jobs/justin/experiment.yaml`
- `jobs/ds2/experiment.yaml`
