# 5. Infrastructure & Cost

UrbanRide runs on AWS as **three self-contained regional cells** (`ap-south-1`, `eu-west-1`, `us-east-1`), each a complete copy of the seven services. Stateless services run on EC2 Spot above a smaller On-Demand floor; everything stateful uses managed services, because self-managing Kafka or Redis on interruptible capacity trades a modest saving for a large operational risk.

Sizing is derived from the agreed platform assumptions: 1,000,000 completed rides/day, 150,000 peak drivers, 37,500 GPS pings/s, a 500 ms end-to-end budget, 300 LKR/USD. All costs are planning estimates from published us-east-1 list prices (September 2026) [1]–[11], not quotations; committed-use discounts apply only where stated.

<!-- Regenerate with: python diagrams/src/05-infrastructure-cost.py (requires the `diagrams` package and graphviz). -->
![UrbanRide deployment and region topology](../diagrams/export/05-infrastructure-cost.png)

*Figure 5. One cell expanded; the other two are identical. Solid arrows are synchronous, dashed asynchronous. Each tier carries its monthly cost, so the figure doubles as a visual form of Section 5.5. Service-to-service call flow is covered by the container diagram in Section 3; database design belongs to Section 2 and the geospatial index layout to Section 4 — this section sizes and prices what they specify.*

## 5.1 Region cells and blast radius

A cell owns its drivers and trips outright — a cell-based architecture [12] in which Route 53 latency routing [13] pins each user to a home cell.

- **Matching never crosses a cell boundary**, because a driver 8,000 km away is never a candidate. The partitioning is a property of the domain, not a constraint we impose.
- **Only two things are global**, neither on the matching path: Identity & Profile reference data via DynamoDB global tables [23], and the S3 analytics/archive lake.
- **Network partition answer:** a severed cell keeps matching, pricing and trip completion running on local state, losing only cross-region profile propagation, which is eventually consistent by design. The failure domain is one region's riders, not the platform.
- **Each cell spans three Availability Zones**, so an AZ loss is absorbed inside the cell by the managed services' own failover.

## 5.2 Compute topology and instance sizing

AWS encodes a machine's shape in its name. In `c7g.2xlarge`: `c` is compute-optimised, `7` the generation, `g` an AWS Graviton (ARM) processor [19], `2xlarge` the size — **8 vCPU / 16 GiB**. `m7g.2xlarge` is the general-purpose sibling with the same 8 vCPU but **32 GiB**. Graviton is used throughout for roughly 20% better price/performance on these Go and JVM workloads.

Every cell is sized so it can absorb the **entire platform peak** of 37,500 pings/s, not merely its own third, so that losing two regions degrades latency rather than capacity. Steady-state figures below are the cell's normal one-third share.

| Service | Workload character | Node type | Purchase model | Steady → peak pods (per cell) |
|---|---|---|---|---|
| API Gateway | Connection-heavy, stateless | c7g.2xlarge (8 vCPU / 16 GiB) | On-Demand floor + Spot burst | 7 → 20 |
| Driver Location Ingestion | Network- and CPU-bound, stateless | c7g.2xlarge (8 vCPU / 16 GiB) | Spot above floor | 11 → 32 |
| Matching Engine | Latency-critical, cache-bound | c7g.2xlarge (8 vCPU / 16 GiB) | 40% On-Demand floor | 8 → 24 |
| Trip Management | Transactional, stateless | m7g.2xlarge (8 vCPU / 32 GiB) | On-Demand floor + Spot | 6 → 24 |
| Surge Pricing | Windowed aggregation | m7g.2xlarge (8 vCPU / 32 GiB) | Spot | 5 → 16 |
| Billing & Notifications | Asynchronous, deferrable | m7g.2xlarge (8 vCPU / 32 GiB) | Spot | 4 → 14 |
| Identity & Profile | Read-mostly | c7g.2xlarge (8 vCPU / 16 GiB) | On-Demand | 3 → 8 |
| Supporting services — fraud, KYC, support tooling, promotions, admin | Mixed internal | m7g.2xlarge (8 vCPU / 32 GiB) | Spot | 12 → 30 |
| Receipts, reconciliation, reports | Low-frequency, bursty | Lambda | Per-request [9] | — |

**Where the pod counts come from.** The ride-request rate is small — ~12 req/s average and ~60/s at the 5x peak — which one pod could absorb. The fleet is therefore sized by the **GPS stream, not ride requests**.

- **Driver Location Ingestion** is the heaviest. In production a pod on two reserved vCPU sustains roughly **1,200 pings/s** once TLS termination, decoding, validation and the Kafka produce are counted — deliberately conservative, since an optimistic per-pod figure is the commonest way a cost model understates compute. A cell's steady third (12,500 pings/s) therefore needs 11 pods, and the full platform peak of 37,500 pings/s needs 32.
- **Matching Engine** is sized off the same stream, because every ping updates the geospatial index; its ~20 matches/s per cell is negligible beside that.
- **Everything else** is sized for redundancy and event fan-out rather than request throughput, with baselines acting as Availability-Zone spread floors.

Across all services a cell runs **22 nodes at its steady floor and 67 at full-platform peak, averaging 45 over a month**. Each cluster carries two node groups — an On-Demand baseline and a Spot burst pool — with pod topology spread so no service is ever entirely on Spot.

**Serverless is confined to low-frequency paths.** Per-request pricing is excellent at receipt volume and poor at 37,500 pings/s, where a long-lived pod holding a WebSocket is roughly two orders of magnitude cheaper. The rule: per-request billing for bursty infrequent work, reserved capacity for sustained streams.

**Spot interruption is a reconnect, not a lost ride.**

- Capacity-optimised allocation [15] spreads each pool across at least four instance types and three AZs.
- The Node Termination Handler [16] cordons and drains on the two-minute interruption notice [14]; pods terminate inside the propagated gRPC deadline.
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
- **Matching Engine is capped at the 48 Kafka partitions defined in Section 3**, beyond which extra consumers sit idle.
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

Per cell, per month, at 730 hours. Each cell is provisioned to absorb the full platform peak, so this is a deliberate upper bound.

| Line | Sizing | Monthly (USD) |
|---|---|---|
| EKS control plane | 1 cluster x $0.10/h [3] | $73 |
| EC2 worker nodes | 45 nodes averaged (floor 22, peak 67), 8 vCPU each: 14 On-Demand @ $0.303/h + 31 Spot @ $0.146/h, blended across the c7g/m7g fleet [1], [2], [11] | $6,401 |
| Amazon MSK brokers | 6 x kafka.m7g.xlarge (4 vCPU) @ $0.408/h + 1 TB broker storage [4] | $1,887 |
| MSK Connect | Debezium CDC runtime for the outbox: 2 workers x 2 MCU @ $0.11/MCU-h [4] | $321 |
| ElastiCache | 3 shards x 2 nodes, cache.r7g.xlarge @ $0.438/h, Valkey engine [5], [24] | $1,918 |
| Aurora PostgreSQL | 1 writer + 2 readers, db.r7g.2xlarge @ $0.956/h + storage and I/O [6], [22] | $2,494 |
| DynamoDB | On-demand read/write units [7] | $600 |
| S3 and lifecycle | Ingest, tiering, requests [8] | $600 |
| Lambda | ~10M invocations [9] | $60 |
| Load balancers | ALB + NLB hourly and capacity units | $400 |
| NAT Gateway | 3 AZs x $0.045/h + data processing [25] | $300 |
| Data transfer | Internet egress + cross-AZ consumption [1] | $1,600 |
| Backups, PITR and DR | Aurora backups, DynamoDB point-in-time recovery, cross-region copies | $600 |
| Security and compliance | WAF, GuardDuty, KMS, Config, Secrets Manager | $1,200 |
| Observability | APM across 45 nodes plus log indexing and traces [26] | $5,000 |
| Supporting services | Fraud, KYC, support tooling, promotions, admin compute and storage | $3,000 |
| **Cell total** | | **≈$26,500** |

| Roll-up | Monthly (USD) |
|---|---|
| 3 region cells | $79,400 |
| Data warehouse, ETL and BI | $6,000 |
| ML training and inference (surge, ETA) | $4,000 |
| Non-production — dev, staging, QA, performance | $12,000 |
| Route 53, CloudFront, cross-region replication | $3,000 |
| CI/CD and artifact storage | $1,000 |
| Subtotal | $105,400 |
| AWS Support — 9% of the first $10k, 7% to $80k, 5% above [27] | $7,100 |
| **AWS platform total** | **≈$112,400** |

| Third-party per ride | Assumption | Monthly | Per ride |
|---|---|---|---|
| Maps and routing | 3 route calls/ride @ $5.00 per 1,000, published list price [28] | $450,000 | 4.50 LKR |
| SMS and OTP | assumed 3% of rides at ~$0.03; per-country rate not published [29] | $27,000 | 0.27 LKR |
| Push notifications | ~10 per ride @ $0.50/million [29] | $150 | <0.01 LKR |

| Cost per completed ride | |
|---|---|
| Rides per month | 30,000,000 |
| AWS infrastructure | $0.00375 = 1.12 LKR |
| Third-party APIs | 4.77 LKR |
| **Total cost per completed ride** | **≈5.89 LKR** |
| Assignment ceiling | 10 LKR |
| **Headroom** | **41% below ceiling** |

Maps is costed at **published list price**, the conservative assumption. At the high-volume tier of $0.75 per 1,000 the same traffic costs $67,500/month and the total falls to **2.07 LKR per ride**, so the maps contract alone moves the result by a factor of nearly three.

## 5.6 Sensitivity and scope

**Maps, not architecture, is the dominant variable.** At list price maps is 4.50 LKR of the 5.89 LKR total — four times the entire AWS bill. Any per-ride figure quoted without stating its maps pricing tier is not comparable with another.

**Spot and the rest of the cost engineering.** At 100% On-Demand, EC2 rises to $9,954/cell and the AWS total to ≈$123,600/month — 1.24 LKR per ride. Removing every optimisation together — no Spot, x86 instead of Graviton, no auto-scaling (provisioned at peak around the clock) and no storage tiering — costs ≈$165,000/month, or **1.65 LKR per ride against 1.12**. The cost engineering therefore removes about **32% of the AWS bill**; the managed data tier and observability, not raw compute, set the floor.

**Cost per ride improves with volume.** Auto-scaling means the bill tracks average load, not peak, and a sustained rise in demand multiplies completed rides faster than cost. The risk runs the other way: at launch-market volumes the ≈$6,700/cell fixed floor dominates.

| Footprint | Rides/day | AWS monthly | AWS per ride | Total with list-price maps |
|---|---|---|---|---|
| 1 cell, minimum viable | 20,000 | ≈$13,400 (4.0M LKR) | 6.70 LKR | 11.47 LKR |
| 1 cell, moderate load | 50,000 | ≈$16,200 (4.9M LKR) | 3.24 LKR | 8.01 LKR |
| 1 cell, at capacity | 150,000 | ≈$21,000 (6.3M LKR) | 1.40 LKR | 6.17 LKR |
| **3 cells (this design)** | **1,000,000** | **≈$112,400 (33.7M LKR)** | **1.12 LKR** | **5.89 LKR** |

A single-market launch at 20,000 rides/day **breaches the 10 LKR ceiling** once list-price maps are included, and is rescued either by a negotiated maps contract or by volume. This is the clearest statement of the design's economics: it is cheap because of scale, not in spite of it, and a fourth cell should be justified by latency or data residency, never by traffic growth alone.

**Currency exposure.** AWS and the maps provider both bill in USD while launch-market revenue is earned in LKR. At 300 LKR/USD the AWS total is 33.7M LKR/month; a 20% rupee depreciation to 360 raises it to **40.5M LKR/month with no change in usage** — a larger swing than the entire Spot saving. The mitigations are commercial: Savings Plans fix the USD rate for the committed baseline, and fares should track the exchange rate.

**Scope.** Payment processing fees are excluded deliberately — at roughly 2.9% of a 400 LKR fare they are about 11.6 LKR per ride on their own, more than the entire ceiling, which confirms the 10 LKR target is a **technology-cost** target rather than a per-ride profit-and-loss line. Against that 400 LKR fare, 5.89 LKR is 1.5% of the fare, or about 7.4% of platform commission at a 20% take rate.

## 5.7 References

Prices are us-east-1 published list prices, accessed September 2026, used as planning estimates rather than quotations. Kafka topic, partition and delivery-guarantee decisions, with their own sources, are given in Section 3.

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

[25] Amazon Web Services, "Amazon VPC pricing." [Online]. Available: https://aws.amazon.com/vpc/pricing/. [Accessed: Sep. 13, 2026].

[26] Amazon Web Services, "Amazon CloudWatch pricing." [Online]. Available: https://aws.amazon.com/cloudwatch/pricing/. [Accessed: Sep. 13, 2026].

[27] Amazon Web Services, "AWS Support plan pricing." [Online]. Available: https://aws.amazon.com/premiumsupport/pricing/. [Accessed: Sep. 13, 2026].

[28] Google, "Google Maps Platform pricing." [Online]. Available: https://developers.google.com/maps/billing-and-pricing/pricing. [Accessed: Sep. 13, 2026].

[29] Amazon Web Services, "Amazon SNS pricing." [Online]. Available: https://aws.amazon.com/sns/pricing/. [Accessed: Sep. 13, 2026].
