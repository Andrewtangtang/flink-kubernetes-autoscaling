# Q4 Benchmark

This experiment compares Justin and DS2 on the state-heavy `q4_unique` Flink
SQL query.

## Configuration

| Setting | Value |
|---|---|
| Producer rate | 40K mixed Nexmark events/s |
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
| Bid Kafka source | 6 | no |
| Auction Kafka source | 3 | no |
| Interval join | 1 | yes |
| Maximum-bid aggregate | 1 | yes |
| Category-average aggregate and sink | 1 | yes |

Each policy has a complete standalone manifest:

- `jobs/justin/experiment.yaml`
- `jobs/ds2/experiment.yaml`
