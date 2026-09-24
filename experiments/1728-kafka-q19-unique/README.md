# Q19 Benchmark

This experiment compares Justin and DS2 on the state-heavy `q19_unique` Flink
SQL query.

The shared rates, resources, and run procedure are in
[`docs/experiment-flow.md`](../../docs/experiment-flow.md). Set `QUERY=q19`
before sourcing `experiments/benchmark-run-env.sh`.

Initial job parallelism:

| Operator | Parallelism | Autoscaled |
|---|---:|:---:|
| Bid Kafka source | 1 | no |
| Rank and sink | 1 | yes |

Choose one standalone manifest:

- `jobs/justin/experiment.yaml`
- `jobs/ds2/experiment.yaml`
