# 5. Infrastructure & Cost

UrbanRide runs on AWS as **three self-contained regional cells** (`ap-south-1`, `eu-west-1`, `us-east-1`), each a complete copy of the seven services. Stateless services run on EC2 Spot above a smaller On-Demand floor; everything stateful uses managed services, because self-managing Kafka or Redis on interruptible capacity trades a modest saving for a large operational risk.

Sizing is derived from the agreed platform assumptions: 1,000,000 completed rides/day, 150,000 peak drivers, 37,500 GPS pings/s, a 500 ms end-to-end budget, 300 LKR/USD. All costs are planning estimates from published us-east-1 list prices (September 2026) [1]–[11], not quotations; committed-use discounts apply only where stated.

<!-- Regenerate with: python diagrams/src/05-infrastructure-cost.py (requires the `diagrams` package and graphviz). -->
![UrbanRide deployment and region topology](../diagrams/export/05-infrastructure-cost.png)

*Figure 5. One cell expanded; the other two are identical. Solid arrows are synchronous, dashed asynchronous. Each tier carries its monthly cost, so the figure doubles as a visual form of §5.5. Service-to-service call flow is covered by the container diagram in §3; database design belongs to §2 and the geospatial index layout to §4 — this section sizes and prices what they specify.*

## 5.1 Region cells and blast radius

A cell owns its drivers and trips outright — a cell-based architecture [12] in which Route 53 latency routing [13] pins each user to a home cell.

- **Matching never crosses a cell boundary**, because a driver 8,000 km away is never a candidate. The partitioning is a property of the domain, not a constraint we impose.
- **Only two things are global**, neither on the matching path: Identity & Profile reference data via DynamoDB global tables [23], and the S3 analytics/archive lake.
- **Network partition answer:** a severed cell keeps matching, pricing and trip completion running on local state, losing only cross-region profile propagation, which is eventually consistent by design. The failure domain is one region's riders, not the platform.
- **Each cell spans three Availability Zones**, so an AZ loss is absorbed inside the cell by the managed services' own failover.

## 5.2 Compute topology and instance sizing

AWS encodes a machine's shape in its name. In `c7g.2xlarge`: `c` is compute-optimised, `7` the generation, `g` an AWS Graviton (ARM) processor [19], `2xlarge` the size — **8 vCPU / 16 GiB**. `m7g.2xlarge` is the general-purpose sibling with the same 8 vCPU but **32 GiB**. Graviton is used throughout for roughly 20% better price/performance on our Go and JVM workloads, which recompile for ARM without effort.

| Service | Workload character | Node type | Purchase model | Baseline → peak pods |
|---|---|---|---|---|
| API Gateway | Connection-heavy, stateless | c7g.2xlarge | On-Demand floor + Spot | 4 → 16 |
| Driver Location Ingestion | Network/CPU bound, stateless | c7g.2xlarge | Spot above floor | 6 → 24 |
| Matching Engine | Latency-critical, cache-bound | c7g.2xlarge | 40% On-Demand floor | 8 → 24 |
| Trip Management | Transactional, stateless | m7g.2xlarge | On-Demand floor + Spot | 4 → 18 |
| Surge Pricing | Windowed aggregation | m7g.2xlarge | Spot | 3 → 12 |
| Billing & Notifications | Asynchronous, deferrable | m7g.2xlarge | Spot | 2 → 10 |
| Identity & Profile | Read-mostly | c7g.2xlarge | On-Demand | 2 → 6 |
| Receipts, reconciliation | Low-frequency, bursty | Lambda | Per-request [9] | — |

**Where the pod counts come from.** The ride-request rate is small — 1M rides/day is ~12 requests/s, ~60/s at the 5x peak — so the fleet is sized by the **GPS stream, not ride requests**. This is also why cost per ride lands so far below the ceiling.

- **Driver Location Ingestion** is heaviest: a cell's steady share is 12,500 pings/s, and one pod on two reserved vCPU sustains ~2,500 pings/s including the WebSocket read and Kafka produce. Five pods carry that, so baseline is six, giving two per AZ. The **24-pod ceiling gives ~60,000 pings/s**, letting one cell absorb the entire 37,500 pings/s platform peak if the other two become unreachable.
- **Matching Engine** is sized off the same stream, because every ping updates the geospatial index. Its 20 matches/s per cell at peak is negligible beside that, which fixes the floor at eight pods.
- **Trip Management, Surge Pricing, Billing** are sized for redundancy and event fan-out, not throughput. Baselines are AZ-spread floors, and the request-rate trigger in §5.3 is a guard that should rarely fire.

**Serverless is confined to low-frequency paths.** Per-request pricing is excellent at receipt volume (~10M invocations/month/cell, ~$60) and poor at 37,500 pings/s, where a long-lived pod holding a WebSocket is roughly two orders of magnitude cheaper. The rule: per-request billing for bursty infrequent work, reserved capacity for sustained streams.

**Spot interruption is a reconnect, not a lost ride.**

- Capacity-optimized allocation [15] spreads each pool across ≥4 instance types and 3 AZs; two node groups per cluster with pod topology spread mean no service is ever entirely on Spot.
- The Node Termination Handler [16] cordons and drains on the two-minute interruption notice [14]; pods terminate inside the gRPC deadline propagated in §3.
- **No ride state lives in a pod** — trip state is in Aurora, positions in Kafka and Valkey — so a reclaimed node costs one client reconnection.
- The On-Demand baseline is covered by one-year Compute Savings Plans [10]; burst capacity is not committed.

## 5.3 Auto-scaling triggers

Horizontal Pod Autoscaler on the metrics below, with Cluster Autoscaler provisioning nodes [17], [18].

| Component | Metric | Scale out | Scale in | Bounds |
|---|---|---|---|---|
| Driver Location Ingestion | CPU, connections/pod | >70% CPU or >8,000 conns for 60 s | <40% for 10 min | 6–24 pods |
| Matching Engine | Consumer lag, p95 latency | Lag >2 s for 60 s, or p95 >120 ms | Lag <0.5 s for 10 min | 8–24 pods |
| Trip Management | Requests/s per pod | >120 rps/pod for 60 s | <50 rps/pod for 10 min | 4–18 pods |
| Surge Pricing | Consumer lag | >5 s | <1 s for 10 min | 3–12 pods |
| Billing & Notifications | Backlog depth | >10,000 events | <1,000 events | 2–10 pods |
| Node groups | Unschedulable pods | Any pod pending >30 s | Node <50% utilised 10 min | 6–60 nodes |

- **Scaling is asymmetric** — fast out, slow in — so a surge arriving in seconds is met immediately while recovery does not flap.
- **Matching Engine is capped at the 48 Kafka partitions defined in §3**, beyond which extra consumers sit idle.
- **Known events are pre-warmed on a schedule** — match fixtures, concert end times, forecast storms — because node provisioning takes two to four minutes, longer than a 5x spike takes to arrive. Reactive scaling handles only the unpredictable remainder.

## 5.4 Storage tiering

At 37,500 pings/s × ~200 bytes, GPS ingestion is **648 GB/day raw**, or ~81 GB/day (2.4 TB/month) batched into compressed Parquet. Tiering that volume matters more than tiering anything else.

| Tier | Data | Store / class | Retention | Rate |
|---|---|---|---|---|
| Hot | Live positions, active trip state | ElastiCache Valkey, DynamoDB | Minutes | — |
| Warm | GPS stream, trip/billing events | MSK broker storage | 1 h / 7 days | $0.10/GB-mo |
| Warm | Operational trips, billing records | Aurora PostgreSQL | 90 days | — |
| Cool | Ride history, Parquet | S3 Standard | 0–30 days | $0.023/GB-mo |
| Cool | Ride history | S3 Standard-IA | 30–90 days | $0.0125/GB-mo |
| Cold | Audit and analytics | S3 Glacier Instant Retrieval | 90 days–1 yr | $0.004/GB-mo |
| Archive | Regulatory retention | S3 Glacier Deep Archive | 1–7 yrs | $0.00099/GB-mo |

Lifecycle policies perform the transitions [20], [21]. Tiering only pays off on aggregated Parquet files, never per-ping objects, because the cheaper classes carry 128 KB minimum billable object sizes and 30/90/180-day minimum durations [21]. At seven-year steady state ~200 TB sits in Deep Archive for **~$205/month per cell**, against over $4,700/month in S3 Standard.

## 5.5 Cost model

Per cell, per month, at 730 hours:

| Line | Sizing | Monthly (USD) |
|---|---|---|
| EKS control plane | 1 cluster × $0.10/h [3] | $73 |
| EC2 worker nodes | 20 nodes averaged, 8 vCPU each: 6 On-Demand @ $0.303/h + 14 Spot @ $0.146/h, blended across the 65% c7g / 35% m7g fleet above [1], [2], [11] | $2,822 |
| Amazon MSK | 6 × kafka.m7g.large (2 vCPU / 8 GiB) @ $0.204/h + 500 GB storage [4] | $944 |
| ElastiCache Valkey | 3 shards × 2 nodes, cache.r7g.large (2 vCPU / 13 GiB) @ $0.219/h [5], [24] | $959 |
| Aurora PostgreSQL | 1 writer + 2 readers, db.r7g.xlarge (4 vCPU / 32 GiB) @ $0.478/h + storage/IO [6], [22] | $1,247 |
| DynamoDB | On-demand, ~100M write units + reads [7] | $200 |
| S3 and lifecycle | Ingest, tiering, requests [8] | $300 |
| Lambda | ~10M invocations [9] | $60 |
| Load balancers and data transfer | Egress and cross-AZ | $500 |
| Observability | Metrics, logs, traces | $600 |
| **Cell total** | | **≈$7,700** |

| Roll-up | Monthly (USD) |
|---|---|
| 3 region cells | $23,100 |
| Global layer — Route 53, CloudFront, cross-region replication, analytics lake, dev/staging | $7,200 |
| **Platform total** | **≈$30,300** |

| Cost per completed ride | |
|---|---|
| Rides per month | 30,000,000 |
| Cost per ride | $0.00101 = **0.30 LKR** |
| Assignment ceiling | 10 LKR |
| **Headroom** | **97% below ceiling** |

## 5.6 Sensitivity and scope

**Spot is worth less than it looks.** At 100% On-Demand, EC2 rises to $4,418/cell and the platform to ≈$35,100/month — 0.35 LKR per ride, still far inside budget. Spot saves 36% of compute but only 14% of the total bill. At this scale the **managed data tier, not compute, dominates cost**; the largest remaining lever is Valkey and Aurora right-sizing, not cheaper instances.

**Cost per ride improves with volume, and degrades sharply without it.** Auto-scaling means the bill tracks average load, not peak, so a 5x spike for three hours a day adds only ~8% to compute — and because a sustained demand increase also multiplies completed rides, the fixed managed tier amortises over more of them. The risk runs the other way: at single-market volumes the ~$3,200/cell managed floor dominates.

| Footprint | Rides/day | Monthly | Cost per ride |
|---|---|---|---|
| 1 cell, minimum viable | 20,000 | ≈$4,500 (1.4M LKR) | 2.25 LKR |
| 1 cell, moderate load | 50,000 | ≈$5,800 (1.7M LKR) | 1.16 LKR |
| 1 cell, at capacity | 150,000 | ≈$7,700 (2.3M LKR) | 0.51 LKR |
| **3 cells (this design)** | **1,000,000** | **≈$30,300 (9.1M LKR)** | **0.30 LKR** |

Per-ride cost is five to seven times worse at launch-market scale, so the architecture is cheap **because of** scale, not in spite of it — though the 10 LKR target is still met from day one. A fourth cell should be justified by latency or data residency, never by traffic growth alone. Single-cell rows scale compute to the stated load and hold the managed tier at its multi-AZ minimum, which sets the floor.

**Currency exposure.** AWS bills in USD while launch-market revenue is earned in LKR. At 300 LKR/USD the platform total is **9.1M LKR/month (~109M LKR/year)**; a 20% rupee depreciation to 360 raises it to **10.9M LKR/month with no change in usage** — a larger swing than the entire Spot saving. Mitigations are commercial, not architectural: Savings Plans fix the USD rate for the committed baseline, and fares should track the exchange rate.

**Scope.** This covers the core backend in the brief. It excludes third-party costs that dominate real ride-hailing unit economics — maps and geocoding calls, payment processor fees, SMS and push delivery — which is what the remaining ~9.7 LKR of headroom absorbs. Against a typical 400 LKR urban fare, the 10 LKR ceiling is 2.5% of the fare and our 0.30 LKR is 0.075%, or about **0.38% of platform commission** at a 20% take rate. The infrastructure target is met with a wide margin; the commercial risk sits outside this boundary.

## 5.7 References

Prices are us-east-1 published list prices, accessed September 2026, used as planning estimates rather than quotations. Kafka topic, partition and delivery-guarantee decisions, with their own sources, are given in §3.

[1] Amazon Web Services, "Amazon EC2 On-Demand pricing." [Online]. Available: https://aws.amazon.com/ec2/pricing/on-demand/. [Accessed: Sep. 13, 2026].

[2] Amazon Web Services, "Amazon EC2 Spot Instances pricing." [Online]. Available: https://aws.amazon.com/ec2/spot/pricing/. [Accessed: Sep. 13, 2026].

[3] Amazon Web Services, "Amazon EKS pricing." [Online]. Available: https://aws.amazon.com/eks/pricing/. [Accessed: Sep. 13, 2026].

[4] Amazon Web Services, "Amazon Managed Streaming for Apache Kafka (MSK) pricing." [Online]. Available: https://aws.amazon.com/msk/pricing/. [Accessed: Sep. 13, 2026].

[5] Amazon Web Services, "Amazon ElastiCache pricing." [Online]. Available: https://aws.amazon.com/elasticache/pricing/. [Accessed: Sep. 13, 2026].

[6] Amazon Web Services, "Amazon Aurora pricing." [Online]. Available: https://aws.amazon.com/rds/aurora/pricing/. [Accessed: Sep. 13, 2026].

[7] Amazon Web Services, "Amazon DynamoDB on-demand capacity mode pricing." [Online]. Available: https://aws.amazon.com/dynamodb/pricing/on-demand/. [Accessed: Sep. 13, 2026].

[8] Amazon Web Services, "Amazon S3 pricing." [Online]. Available: https://aws.amazon.com/s3/pricing/. [Accessed: Sep. 13, 2026].

[9] Amazon Web Services, "AWS Lambda pricing." [Online]. Available: https://aws.amazon.com/lambda/pricing/. [Accessed: Sep. 13, 2026].

[10] Amazon Web Services, "Compute Savings Plans pricing." [Online]. Available: https://aws.amazon.com/savingsplans/compute-pricing/. [Accessed: Sep. 13, 2026].

[11] Vantage, "c7g.2xlarge pricing and specifications," Amazon EC2 Instance Comparison. [Online]. Available: https://instances.vantage.sh/aws/ec2/c7g.2xlarge. [Accessed: Sep. 13, 2026].

[12] Amazon Web Services, "Reducing the scope of impact with cell-based architecture," AWS Well-Architected Framework. [Online]. Available: https://docs.aws.amazon.com/wellarchitected/latest/reducing-scope-of-impact-with-cell-based-architecture/reducing-scope-of-impact-with-cell-based-architecture.html. [Accessed: Sep. 13, 2026].

[13] Amazon Web Services, "Latency-based routing," Amazon Route 53 Developer Guide. [Online]. Available: https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/routing-policy-latency.html. [Accessed: Sep. 13, 2026].

[14] Amazon Web Services, "Spot Instance interruptions," Amazon EC2 User Guide. [Online]. Available: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-interruptions.html. [Accessed: Sep. 13, 2026].

[15] Amazon Web Services, "Allocation strategies for Spot Instances," Amazon EC2 User Guide. [Online]. Available: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-fleet-allocation-strategy.html. [Accessed: Sep. 13, 2026].

[16] Amazon Web Services, "aws-node-termination-handler," GitHub repository. [Online]. Available: https://github.com/aws/aws-node-termination-handler. [Accessed: Sep. 13, 2026].

[17] Amazon Web Services, "Scale cluster compute with Karpenter and Cluster Autoscaler," Amazon EKS User Guide. [Online]. Available: https://docs.aws.amazon.com/eks/latest/userguide/autoscaling.html. [Accessed: Sep. 13, 2026].

[18] The Kubernetes Authors, "Horizontal Pod Autoscaling," Kubernetes Documentation. [Online]. Available: https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/. [Accessed: Sep. 13, 2026].

[19] Amazon Web Services, "AWS Graviton processors." [Online]. Available: https://aws.amazon.com/ec2/graviton/. [Accessed: Sep. 13, 2026].

[20] Amazon Web Services, "Understanding and managing Amazon S3 storage classes," Amazon S3 User Guide. [Online]. Available: https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html. [Accessed: Sep. 13, 2026].

[21] Amazon Web Services, "Considerations for Amazon S3 lifecycle transitions," Amazon S3 User Guide. [Online]. Available: https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-transition-general-considerations.html. [Accessed: Sep. 13, 2026].

[22] Amazon Web Services, "Amazon Aurora DB clusters," Amazon Aurora User Guide. [Online]. Available: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Overview.html. [Accessed: Sep. 13, 2026].

[23] Amazon Web Services, "Global tables: multi-Region replication for DynamoDB," Amazon DynamoDB Developer Guide. [Online]. Available: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GlobalTables.html. [Accessed: Sep. 13, 2026].

[24] Amazon Web Services, "What is Valkey?," Amazon ElastiCache. [Online]. Available: https://aws.amazon.com/elasticache/what-is-valkey/. [Accessed: Sep. 13, 2026].
