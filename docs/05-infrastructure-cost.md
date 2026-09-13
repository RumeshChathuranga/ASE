# 5. Infrastructure & Cost

UrbanRide runs on AWS as **three self-contained regional cells** (`ap-south-1`, `eu-west-1`, `us-east-1`), each holding a complete copy of the seven services with its own EKS cluster, Kafka cluster, geospatial cache and relational store. Stateless services run on EC2 Spot capacity above a smaller On-Demand floor; everything stateful uses managed services, because self-managing Kafka or Redis on interruptible capacity trades a modest saving for a large operational risk. Sizing below is derived from the `FROZEN.md` planning figures — 1,000,000 completed rides/day, 150,000 active drivers at peak, 37,500 GPS pings/s, a 500 ms end-to-end budget and 300 LKR/USD. Costs are planning estimates built from published us-east-1 list prices checked in September 2026; they are not quotations, and committed-use discounts are applied only where stated.

<!-- Regenerate with: python diagrams/src/05-infrastructure-cost.py (requires the `diagrams` package and graphviz). -->
![UrbanRide deployment and region topology](../diagrams/export/05-infrastructure-cost.png)

*Figure 5. One region cell shown expanded; the other two are identical. Solid arrows show synchronous calls, dashed arrows asynchronous event flow. Each tier is annotated with its monthly cost, so the figure doubles as a visual breakdown of the cost model in §5.5. Service-to-service call flow is deliberately omitted — that is Member 3's container diagram. Database design belongs to Member 2 and the geospatial index layout to Member 4; this section sizes and prices what they specify.*

## 5.1 Region cells and blast radius

A cell owns its drivers and trips outright. This is a [cell-based architecture](https://docs.aws.amazon.com/wellarchitected/latest/reducing-scope-of-impact-with-cell-based-architecture/reducing-scope-of-impact-with-cell-based-architecture.html): [Route 53 latency routing](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/routing-policy-latency.html) pins each rider and driver to a home cell, and matching never crosses a cell boundary because a driver 8,000 km away is never a candidate — the partitioning is a property of the domain, not a constraint we impose. Only two things are global, and neither sits on the matching path: Identity & Profile reference data replicated through DynamoDB global tables, and the analytics and archive lake in S3 with cross-region replication.

This is the answer to the network-partition question. A cell severed from the others keeps matching, pricing and trip completion running on local state; it loses only cross-region profile propagation, which is eventually consistent by design. The failure domain is one region's riders, not the platform. Each cell spans three Availability Zones, so an AZ loss is absorbed inside the cell by the managed services' own failover.

## 5.2 Compute topology and instance sizing

AWS encodes a machine's shape in its name, so it is worth decoding the two used here before the table. In `c7g.2xlarge`, the `c` means compute-optimised, `7` is the hardware generation, `g` means an AWS [Graviton](https://aws.amazon.com/ec2/graviton/) processor (ARM, the same architecture as a phone chip), and `2xlarge` is the size — **8 vCPU and 16 GiB of memory**. `m7g.2xlarge` is the general-purpose sibling: the same 8 vCPU but **32 GiB**, used where a service holds more state per in-flight request. Graviton is chosen throughout because it delivers roughly 20% better price/performance than the equivalent x86 instance, and our services are Go and JVM workloads that recompile for ARM without effort.

| Service | Workload character | Node type | Purchase model | Baseline → peak pods (per cell) |
|---|---|---|---|---|
| API Gateway | Connection-heavy, stateless | c7g.2xlarge (8 vCPU / 16 GiB) | On-Demand floor + Spot burst | 4 → 16 |
| Driver Location Ingestion | Network and CPU bound, stateless | c7g.2xlarge (8 vCPU / 16 GiB) | Spot above floor | 6 → 24 |
| Matching Engine | Latency-critical, cache-bound | c7g.2xlarge (8 vCPU / 16 GiB) | 40% On-Demand floor | 8 → 24 |
| Trip Management | Transactional, stateless | m7g.2xlarge (8 vCPU / 32 GiB) | On-Demand floor + Spot | 4 → 18 |
| Surge Pricing | Windowed aggregation | m7g.2xlarge (8 vCPU / 32 GiB) | Spot | 3 → 12 |
| Billing & Notifications | Asynchronous, deferrable | m7g.2xlarge (8 vCPU / 32 GiB) | Spot | 2 → 10 |
| Identity & Profile | Read-mostly | c7g.2xlarge (8 vCPU / 16 GiB) | On-Demand | 2 → 6 |
| Receipts, reconciliation, reports | Low-frequency, bursty | Lambda (serverless) | Per-request | — |

**Where these pod counts come from.** The ride-request rate is genuinely small: 1,000,000 rides/day is about 12 requests/s on average and 60/s at the 5x peak, which one pod could absorb on its own. Almost the whole fleet is therefore sized by the **GPS stream, not by ride requests** — worth stating plainly, because it is also the reason the cost per ride in §5.5 lands so far below the ceiling.

Driver Location Ingestion is the heaviest consumer. 37,500 pings/s spread over three cells is 12,500/s per cell, and one pod on two reserved vCPU sustains roughly 2,500 pings/s once the WebSocket read and the Kafka produce are counted — five pods would carry the average, so the baseline is six, giving two per Availability Zone. The peak of 24 covers a 5x increase in reporting drivers. Matching Engine is sized off the same stream rather than its own request rate, because every ping has to update the geospatial index; the 20 matches/s per cell at peak is negligible beside that, which is what fixes its floor at eight pods. Trip Management, Surge Pricing and Billing & Notifications are sized for redundancy and event fan-out instead of request throughput — their baselines are AZ-spread floors, and the request-rate trigger in §5.3 is a guard that should rarely fire in normal operation.

Each cluster carries two node groups — an On-Demand baseline and a Spot burst pool — with pod topology spread constraints so that no service ever runs entirely on Spot.

**Serverless is deliberately confined to low-frequency paths.** Per-request pricing is excellent at receipt volume (≈10M invocations/month/cell, ≈$60) and poor at 37,500 pings/s, where a long-lived pod holding a WebSocket is roughly two orders of magnitude cheaper than an invocation per ping. The rule is per-request billing for bursty, infrequent work; reserved capacity for sustained streams.

**Spot interruption is a reconnect, not a lost ride.** [Capacity-optimized allocation](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-fleet-allocation-strategy.html) spreads each pool across at least four instance types and three AZs; the [Node Termination Handler](https://github.com/aws/aws-node-termination-handler) cordons and drains on the [two-minute interruption notice](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-interruptions.html); pods terminate gracefully inside the gRPC deadline Member 3 propagates. Critically, no ride state lives in a pod — trip state is in Aurora, driver positions in Kafka and Valkey — so a reclaimed node costs one client reconnection. The On-Demand baseline is covered by one-year Compute Savings Plans; burst capacity is not committed.

## 5.3 Auto-scaling triggers

| Component | Metric | Scale out | Scale in | Bounds |
|---|---|---|---|---|
| Driver Location Ingestion | CPU, WebSocket connections/pod | >70% CPU or >8,000 connections for 60 s | <40% for 10 min | 6–24 pods |
| Matching Engine | Consumer lag, p95 match latency | Lag >2 s for 60 s, or p95 >120 ms | Lag <0.5 s for 10 min | 8–24 pods |
| Trip Management | Requests/s per pod | >120 rps/pod for 60 s | <50 rps/pod for 10 min | 4–18 pods |
| Surge Pricing | Consumer lag | >5 s | <1 s for 10 min | 3–12 pods |
| Billing & Notifications | Backlog depth | >10,000 events | <1,000 events | 2–10 pods |
| Node groups | Unschedulable pods | Any pod pending >30 s | Node <50% utilised 10 min | 6–60 nodes |

Two design rules govern the table. First, scaling is **asymmetric** — fast out, slow in — so a surge that arrives in seconds is met immediately while recovery does not flap. Second, the Matching Engine's ceiling is capped at Member 3's 48 partitions, beyond which extra consumers sit idle. Because provisioning a new node takes two to four minutes, which is longer than a 5x spike takes to arrive, **scheduled scaling pre-warms capacity for known events** — match fixtures, concert end times, forecast storms — and reactive scaling handles only the unpredictable remainder.

## 5.4 Storage tiering

At 37,500 pings/s × ~200 bytes, GPS ingestion is **648 GB/day raw**, or roughly 81 GB/day (2.4 TB/month) once batched into compressed Parquet. Tiering that volume matters more than tiering anything else in the system.

| Tier | Data | Store / class | Retention | Rate |
|---|---|---|---|---|
| Hot | Live driver positions, active trip state | ElastiCache Valkey, DynamoDB | Minutes | — |
| Warm | GPS stream, trip and billing events | MSK broker storage | 1 h / 7 days | $0.10/GB-mo |
| Warm | Operational trips, billing records | Aurora PostgreSQL | 90 days | — |
| Cool | Ride history, Parquet | S3 Standard | 0–30 days | $0.023/GB-mo |
| Cool | Ride history | S3 Standard-IA | 30–90 days | $0.0125/GB-mo |
| Cold | Audit and analytics | S3 Glacier Instant Retrieval | 90 days–1 yr | $0.004/GB-mo |
| Archive | Regulatory retention | S3 Glacier Deep Archive | 1–7 yrs | $0.00099/GB-mo |

Lifecycle policies perform the transitions automatically. Tiering only pays off on aggregated Parquet files, never on per-ping objects, because the cheaper classes carry [128 KB minimum billable object sizes and 30/90/180-day minimum storage durations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-transition-general-considerations.html). At seven-year steady state roughly 200 TB sits in Deep Archive for about $205/month per cell — the same data would cost over $4,700/month in S3 Standard.

## 5.5 Cost model

Monthly, per cell, at 730 hours:

| Line | Sizing | Monthly (USD) |
|---|---|---|
| EKS control plane | 1 cluster × $0.10/h | $73 |
| EC2 worker nodes | 20 nodes averaged over the month, 8 vCPU each: 6 On-Demand at $0.303/h + 14 Spot at $0.146/h. Rates are blended across the 65% c7g / 35% m7g fleet implied by the pod counts above | $2,822 |
| Amazon MSK | 6 brokers, kafka.m7g.large (2 vCPU / 8 GiB), at $0.204/h + 500 GB broker storage | $944 |
| ElastiCache Valkey | 3 shards × 2 nodes, cache.r7g.large (2 vCPU / 13 GiB), at $0.219/h | $959 |
| Aurora PostgreSQL | 1 writer + 2 readers, db.r7g.xlarge (4 vCPU / 32 GiB), at $0.478/h + storage and I/O | $1,247 |
| DynamoDB | On-demand, ≈100M write units + reads | $200 |
| S3 and lifecycle | Ingest, tiering, requests | $300 |
| Lambda | ≈10M invocations | $60 |
| Load balancers and data transfer | Egress and cross-AZ | $500 |
| Observability | Metrics, logs, traces | $600 |
| **Cell total** | | **≈$7,700** |

| Roll-up | Monthly (USD) |
|---|---|
| 3 region cells | $23,100 |
| Global layer — Route 53, CloudFront, cross-region replication, analytics lake, dev and staging | $7,200 |
| **Platform total** | **≈$30,300** |

| Cost per completed ride | |
|---|---|
| Rides per month | 30,000,000 |
| Cost per ride | $0.00101 = **0.30 LKR** |
| Assignment ceiling | 10 LKR |
| **Headroom** | **97% below ceiling** |

## 5.6 Sensitivity and scope

**Spot is worth less than it looks.** At 100% On-Demand, EC2 rises to $4,418/cell and the platform to ≈$35,100/month — 0.35 LKR per ride, still far inside budget. Spot saves 36% of compute but only 14% of the total bill. The honest conclusion is that at this scale the **managed data tier, not compute, dominates cost**; the largest remaining lever is Valkey and Aurora right-sizing, not chasing cheaper instances.

**Surge lowers cost per ride.** Auto-scaling means the monthly bill tracks average load, not peak, so a 5x spike sustained three hours a day adds only about 8% to compute. A sustained increase in demand also multiplies completed rides, so the fixed managed-service tier is amortised over more of them. The per-ride figure is most at risk in the opposite case — prolonged low utilisation, where the fixed floor of roughly $3,200/cell in managed services dominates. A fourth cell should therefore be justified by latency or data-residency requirements, never by traffic growth alone.

**Scope.** This figure covers the core backend defined in the brief. It excludes third-party costs that dominate real ride-hailing unit economics — maps and geocoding API calls, payment processor fees, SMS and push delivery — which are precisely what the remaining ~9.7 LKR of headroom absorbs. The infrastructure target is met with a wide margin; the commercial risk sits outside this boundary.

## 5.7 References

All prices are **us-east-1 published list prices, accessed September 2026**, and are used as planning estimates rather than quotations.

### Sources by role

**Pricing sources**

1. [Amazon EC2 On-Demand pricing](https://aws.amazon.com/ec2/pricing/on-demand/) and [EC2 Spot pricing](https://aws.amazon.com/ec2/spot/pricing/) — c7g/m7g.2xlarge rates.
2. [Amazon EKS pricing](https://aws.amazon.com/eks/pricing/) — $0.10 per cluster-hour.
3. [Amazon MSK pricing](https://aws.amazon.com/msk/pricing/) — kafka.m7g.large broker-hour and $0.10/GB-month broker storage.
4. [Amazon ElastiCache pricing](https://aws.amazon.com/elasticache/pricing/) — cache.r7g.large node-hour.
5. [Amazon Aurora pricing](https://aws.amazon.com/rds/aurora/pricing/) — db.r7g.xlarge instance-hour, storage and I/O.
6. [Amazon DynamoDB on-demand pricing](https://aws.amazon.com/dynamodb/pricing/on-demand/) — write and read request units.
7. [Amazon S3 pricing](https://aws.amazon.com/s3/pricing/) — Standard, Standard-IA, Glacier Instant Retrieval and Glacier Deep Archive.
8. [AWS Lambda pricing](https://aws.amazon.com/lambda/pricing/) — per-request and per-GB-second rates.
9. [Compute Savings Plans pricing](https://aws.amazon.com/savingsplans/compute-pricing/) — commitment applied to the On-Demand baseline only.
10. [Vantage EC2 instance comparison](https://instances.vantage.sh/aws/ec2/c7g.2xlarge) — used to cross-check per-instance On-Demand and Spot rates.

**Architecture and operational references**

11. [Reducing the scope of impact with cell-based architecture](https://docs.aws.amazon.com/wellarchitected/latest/reducing-scope-of-impact-with-cell-based-architecture/reducing-scope-of-impact-with-cell-based-architecture.html), AWS Well-Architected.
12. [Route 53 latency-based routing](https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/routing-policy-latency.html).
13. [EC2 Spot interruption notices](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-interruptions.html) and [Spot allocation strategies](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-fleet-allocation-strategy.html).
14. [AWS Node Termination Handler](https://github.com/aws/aws-node-termination-handler).
15. [Amazon EKS autoscaling](https://docs.aws.amazon.com/eks/latest/userguide/autoscaling.html) and [Kubernetes Horizontal Pod Autoscaler](https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/).
16. [AWS Graviton processors](https://aws.amazon.com/ec2/graviton/).
17. [S3 storage classes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html) and [lifecycle transition constraints](https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-transition-general-considerations.html).
18. [Amazon Aurora overview](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Overview.html) and [DynamoDB global tables](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GlobalTables.html).
19. [Amazon ElastiCache for Valkey](https://aws.amazon.com/elasticache/what-is-valkey/).
20. Kafka topic, partition and delivery-guarantee decisions are owned by Member 3; see `03-communication-streaming.md` for those sources.

### IEEE-style reference list

[1] Amazon Web Services, "Amazon EC2 On-Demand pricing." [Online]. Available: https://aws.amazon.com/ec2/pricing/on-demand/. [Accessed: Sep. 12, 2026].

[2] Amazon Web Services, "Amazon EC2 Spot Instances pricing." [Online]. Available: https://aws.amazon.com/ec2/spot/pricing/. [Accessed: Sep. 12, 2026].

[3] Amazon Web Services, "Amazon EKS pricing." [Online]. Available: https://aws.amazon.com/eks/pricing/. [Accessed: Sep. 12, 2026].

[4] Amazon Web Services, "Amazon Managed Streaming for Apache Kafka (MSK) pricing." [Online]. Available: https://aws.amazon.com/msk/pricing/. [Accessed: Sep. 12, 2026].

[5] Amazon Web Services, "Amazon ElastiCache pricing." [Online]. Available: https://aws.amazon.com/elasticache/pricing/. [Accessed: Sep. 12, 2026].

[6] Amazon Web Services, "Amazon Aurora pricing." [Online]. Available: https://aws.amazon.com/rds/aurora/pricing/. [Accessed: Sep. 12, 2026].

[7] Amazon Web Services, "Amazon DynamoDB on-demand capacity mode pricing." [Online]. Available: https://aws.amazon.com/dynamodb/pricing/on-demand/. [Accessed: Sep. 12, 2026].

[8] Amazon Web Services, "Amazon S3 pricing." [Online]. Available: https://aws.amazon.com/s3/pricing/. [Accessed: Sep. 12, 2026].

[9] Amazon Web Services, "AWS Lambda pricing." [Online]. Available: https://aws.amazon.com/lambda/pricing/. [Accessed: Sep. 12, 2026].

[10] Amazon Web Services, "Compute Savings Plans pricing." [Online]. Available: https://aws.amazon.com/savingsplans/compute-pricing/. [Accessed: Sep. 12, 2026].

[11] Vantage, "c7g.2xlarge pricing and specifications," Amazon EC2 Instance Comparison. [Online]. Available: https://instances.vantage.sh/aws/ec2/c7g.2xlarge. [Accessed: Sep. 12, 2026].

[12] Amazon Web Services, "Reducing the scope of impact with cell-based architecture," AWS Well-Architected Framework. [Online]. Available: https://docs.aws.amazon.com/wellarchitected/latest/reducing-scope-of-impact-with-cell-based-architecture/reducing-scope-of-impact-with-cell-based-architecture.html. [Accessed: Sep. 12, 2026].

[13] Amazon Web Services, "Latency-based routing," Amazon Route 53 Developer Guide. [Online]. Available: https://docs.aws.amazon.com/Route53/latest/DeveloperGuide/routing-policy-latency.html. [Accessed: Sep. 12, 2026].

[14] Amazon Web Services, "Spot Instance interruptions," Amazon EC2 User Guide. [Online]. Available: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/spot-interruptions.html. [Accessed: Sep. 12, 2026].

[15] Amazon Web Services, "Allocation strategies for Spot Instances," Amazon EC2 User Guide. [Online]. Available: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-fleet-allocation-strategy.html. [Accessed: Sep. 12, 2026].

[16] Amazon Web Services, "aws-node-termination-handler," GitHub repository. [Online]. Available: https://github.com/aws/aws-node-termination-handler. [Accessed: Sep. 12, 2026].

[17] Amazon Web Services, "Scale cluster compute with Karpenter and Cluster Autoscaler," Amazon EKS User Guide. [Online]. Available: https://docs.aws.amazon.com/eks/latest/userguide/autoscaling.html. [Accessed: Sep. 12, 2026].

[18] The Kubernetes Authors, "Horizontal Pod Autoscaling," Kubernetes Documentation. [Online]. Available: https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/. [Accessed: Sep. 12, 2026].

[19] Amazon Web Services, "AWS Graviton processors." [Online]. Available: https://aws.amazon.com/ec2/graviton/. [Accessed: Sep. 12, 2026].

[20] Amazon Web Services, "Understanding and managing Amazon S3 storage classes," Amazon S3 User Guide. [Online]. Available: https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html. [Accessed: Sep. 12, 2026].

[21] Amazon Web Services, "Considerations for Amazon S3 lifecycle transitions," Amazon S3 User Guide. [Online]. Available: https://docs.aws.amazon.com/AmazonS3/latest/userguide/lifecycle-transition-general-considerations.html. [Accessed: Sep. 12, 2026].

[22] Amazon Web Services, "Amazon Aurora DB clusters," Amazon Aurora User Guide. [Online]. Available: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Overview.html. [Accessed: Sep. 12, 2026].

[23] Amazon Web Services, "Global tables: multi-Region replication for DynamoDB," Amazon DynamoDB Developer Guide. [Online]. Available: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GlobalTables.html. [Accessed: Sep. 12, 2026].

[24] Amazon Web Services, "What is Valkey?," Amazon ElastiCache. [Online]. Available: https://aws.amazon.com/elasticache/what-is-valkey/. [Accessed: Sep. 12, 2026].
