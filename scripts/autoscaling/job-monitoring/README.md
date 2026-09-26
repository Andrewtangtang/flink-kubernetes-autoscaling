# Live autoscaling observers

These read-only tools use the current `kubectl` context. Start the local forwards
after the services exist:

```bash
scripts/autoscaling/job-monitoring/port-forward.sh start
scripts/autoscaling/job-monitoring/port-forward.sh status
```

The helper forwards `flink-rest` in `default` to `127.0.0.1:18082` and, when
installed, Prometheus and Grafana in `manager` to ports `19091` and `3001`. A
missing service is reported and skipped. Set `FLINK_NAMESPACE`,
`MONITORING_NAMESPACE`, the corresponding `*_LOCAL_PORT`, or
`PORT_FORWARD_ADDRESS` to override these defaults. Only processes started by
this helper are stopped by `port-forward.sh stop`; the default address is
loopback, not a public interface.

```bash
scripts/autoscaling/job-monitoring/observe-scaling.py --follow
scripts/autoscaling/job-monitoring/observe-flink-metrics.py --once
scripts/autoscaling/job-monitoring/observe-flink-metrics.py --interval 5
```

`observe-scaling.py` reads autoscaler ConfigMap history and needs `kubectl` and
PyYAML. It labels Justin capacity as a window average and shows the DS2
decision inputs separately from live throughput. `observe-flink-metrics.py`
uses Flink REST and Prometheus; CPU and memory require Prometheus to scrape
container metrics, while task rates require Flink metrics. Its optional
`--total-events` and `--source-event-share` show replay progress. Kafka reader
offsets make that progress rescale-safe when available; otherwise the displayed
source-record count covers only the current job attempt.

Both observers support `--json` for machine-readable output. Run each with
`--help` for the remaining filters and endpoint overrides.
