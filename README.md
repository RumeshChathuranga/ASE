# UrbanRide — Software Architecture Group Assignment

Architecture Design Document (ADD) and presentation deck for **UrbanRide**, a ride-hailing backend for 1M completed rides/day, sub-500ms transaction latency, surge absorption, and a ceiling of 10 LKR per completed ride.

**Due:** Midnight, Sunday 13 September 2026

---

## How this repo is organised

```
urbanride-architecture/
├── README.md
├── FROZEN.md                        # shared constants — agreed at kickoff, never edited alone
├── docs/
│   ├── 01-context-decomposition.md      # Member 1
│   ├── 02-data-transactions.md          # Member 2
│   ├── 03-communication-streaming.md    # Member 3
│   ├── 04-geospatial-latency.md         # Member 4
│   └── 05-infrastructure-cost.md        # Member 5
├── diagrams/
│   ├── src/                         # editable source, one file per member
│   └── export/                      # PNG/SVG used in the ADD
├── slides/                          # 2 slides per member, same template
└── ADD-final.pdf
```

**One member, one file, one diagram.** Nobody edits another member's `docs/` file or diagram. This is what makes the split truly parallel.

---

## Kickoff (20 minutes, all five, before anyone writes)

Three things get agreed and written into `FROZEN.md`. After this call, no member depends on any other member.

1. **Service names** — taken straight from the brief, so there is nothing to invent:

   | Core services | Supporting |
   |---|---|
   | Trip Management | API Gateway |
   | Driver Location Ingestion | Identity & Profile |
   | Matching Engine | |
   | Surge Pricing | |
   | Billing & Notifications | |

2. **Diagram tool and template** — pick one (draw.io, Mermaid, or Structurizr). Agree box shape, arrow style, and colour for sync vs async calls. Five diagrams drawn separately must look like one set.

3. **Shared numbers** — every section quotes these, so no two sections contradict each other. Suggested starting values, adjust once as a group:

   | Quantity | Value |
   |---|---|
   | Completed rides/day | 1,000,000 |
   | Average ride request rate | ~12 req/s |
   | Peak surge multiplier | 5x → ~60 req/s |
   | Active drivers at peak | 150,000 |
   | GPS ping interval | 4 seconds |
   | Resulting ingestion rate | ~37,500 pings/s |
   | Latency budget (end to end) | 500 ms |
   | Daily cost ceiling | 10M LKR (~$33k/day at 300 LKR/USD) |

---

## Workload Split

Each parcel is the same shape: **one document section + one diagram + two slides + three Q&A questions.**

| | Member 1 | Member 2 | Member 3 | Member 4 | Member 5 |
|---|---|---|---|---|---|
| **Section** | Context & Decomposition | Data Design & Transactions | Communication & Streaming | Geospatial Matching & Latency | Infrastructure & Cost |
| **Diagram** | C4 Level 1 — System Context | Saga sequence diagram | C4 Level 2 — Container | C4 Level 3 — Component (Matching Engine) | Deployment / region topology |
| **Covers** | Bounded context + data ownership for each of the five services; what was deliberately *not* split and why; CAP stance per service (AP for location, CP for billing) | Database-per-service table with justification; Saga for ride → fare → payment with compensating actions; idempotency keys; outbox pattern; lock contention on driver assignment | gRPC vs REST vs WebSocket decision table with the chosen protocol per link; Kafka topic design for GPS pings (partition key, retention, consumer groups); backpressure; dead-letter queues; delivery guarantees | Redis Geo vs H3 vs S2 choice and index layout; latency budget table summing under 500ms; cache invalidation on driver state change; circuit breakers and rate limiters on the matching path | Instance sizing per service with rationale; spot/preemptible for stateless, serverless for low-frequency paths; auto-scaling triggers (metric + threshold); hot/warm/cold storage tiering; cost model ending in a cost-per-ride figure in LKR |
| **Q&A to prep** | Why these boundaries, why not a monolith | Double-booking a driver, payment failing after ride completes | Kafka partition hot-spotting, consumer lag under spike | Geo-index cache invalidation, 5x spike on matching | Spot capacity reclaimed mid-ride, cost behaviour under surge |
| **Rubric reached** | ~12% | ~13% | ~13% | ~14% | 20% |

Presentation clarity (15%) and Panel Defense (15%) are split five ways — roughly 3% + 3% each — through the two slides and three questions everyone contributes.

**On the remaining imbalance:** Member 5's cost section is the largest single write-up and it maps to a whole 20% rubric category that cannot be sensibly cut in half. To even it out, Member 5 does no assembly work. Member 1 merges the ADD into the final PDF; Member 3 merges the slide deck. Both are mechanical jobs that fall to the two lightest sections.

---

## Timeline

| When | What |
|---|---|
| Sat morning | 20-minute kickoff call. `FROZEN.md` committed. |
| Sat, rest of day | Five people writing five files. No coordination needed. |
| Sat evening | Everyone pushes. Read the other four sections once, looking only for contradictions. |
| Sun morning | Fix contradictions. Export diagrams. |
| Sun afternoon | Member 1 assembles ADD, Member 3 assembles deck, group dry-runs the 10 minutes. |
| Sun evening | Buffer. Submit. |

---

## Consistency Checklist

- [ ] Service names identical in all five sections and all five diagrams
- [ ] Every number quoted traces back to `FROZEN.md`
- [ ] Latency budget adds to under 500ms
- [ ] Cost per ride calculated, shown as a table, stated below 10 LKR
- [ ] Every service has a named datastore and a named protocol
- [ ] Diagrams share one visual style
- [ ] Deck runs in 10 minutes when spoken aloud

---

## Conventions

- Branch per member: `feat/m<n>-<section>`, PR into `main`
- Diagram sources committed alongside exports
- Markdown throughout; ADD assembled to PDF at the end
