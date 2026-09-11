# Normalized workload manifests

These are independent copies of the checkpoint-aware Justin and DS2 manifests.
The original `jobs/` tree remains the paper-reproduction profile.

All queries use:

- TaskManager: 4 CPU, 3 GiB, four slots;
- Justin memory levels M0-M2 (`memory.max-level=3`);
- 3-minute stabilization and 2-minute metrics windows;
- bid source P6 wherever bids are consumed;
- auction source P3 wherever auctions are consumed; and
- initial downstream parallelism P1 with per-vertex maximum P12.

The fixed sources remain excluded from autoscaling. The source IDs are
`cbc357ccb763df2852fee8c4fc7d55f2` for bid and
`6cdc5bb954874d922eaee11a8e7b5dd5` for auction.

Producer settings are external to the Flink manifests. To provide about 55,000
query events/s, use:

| Queries | Mixed producer rate | Query event share |
|---|---:|---:|
| Q4, Q9, Q20 | 56,123 events/s | 0.98 |
| Q18, Q19 | 59,783 events/s | 0.92 |

These manifests are AWS templates. They use S3 for durable Flink state,
`/data/flink/rocksdb` for node-local NVMe, and the `workload=flink` node label.
Render the image, bucket, Kafka endpoint, and run ID through the existing run
scripts:

```bash
cp experiments/1729-checkpoint-aware-rescaling/aws.env.example /tmp/flink-aws.env
# Edit /tmp/flink-aws.env; never add credentials.
source /tmp/flink-aws.env
export QUERY=q20 POLICY=justin RUN_ID=aws-q20-justin-01
source experiments/1729-checkpoint-aware-rescaling/run-env.sh
experiments/1729-checkpoint-aware-rescaling/render-job.sh
```
