# Q20 Benchmark

This experiment compares Justin and DS2 on the state-heavy `q20_unique` Flink
SQL query.

## Configuration

| Setting | Justin | DS2 |
|---|---:|---:|
| Producer rate | 60K mixed Nexmark events/s | 60K mixed Nexmark events/s |
| Generated events | 100M | 100M |
| TaskManager resources | 8 CPU, 3 GiB, 8 slots | 8 CPU, 3 GiB, 8 slots |
| Managed memory | 1264 MiB | 1264 MiB |
| Metrics window | 2 minutes | 2 minutes |
| Stabilization interval | 3 minutes | 3 minutes |
| Maximum parallelism | 12 | 20 |

Initial job parallelism:

| Operator | Parallelism | Autoscaled |
|---|---:|:---:|
| Bid Kafka source | 12 | no |
| Auction Kafka source | 1 | no |
| Interval join | 1 | yes |

Each policy has a complete standalone manifest:

- `jobs/justin/experiment.yaml`
- `jobs/ds2/experiment.yaml`
