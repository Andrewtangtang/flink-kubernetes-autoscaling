# Q9 Benchmark

This experiment compares Justin and DS2 on the state-heavy `q9_unique` Flink
SQL query.

The shared rates, resources, and run procedure are in
[`docs/experiment-flow.md`](../../docs/experiment-flow.md). Set `QUERY=q9`
before sourcing `experiments/benchmark-run-env.sh`.

Initial job parallelism:

| Operator | Parallelism | Autoscaled |
|---|---:|:---:|
| Bid Kafka source | 6 | no |
| Auction Kafka source | 3 | no |
| Interval join | 1 | yes |
| Rank and sink | 1 | yes |

Choose one standalone manifest:

- `jobs/justin/experiment.yaml`
- `jobs/ds2/experiment.yaml`
