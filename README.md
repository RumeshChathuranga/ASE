# UrbanRide — Software Architecture Group Assignment

Architecture Design Document (ADD) and presentation deck for **UrbanRide**, a global ride-hailing backend designed for 1M completed rides/day, sub-500ms transaction latency, surge absorption, and a cost ceiling of 10 LKR per completed ride.

**Due:** Midnight, Sunday 13 September 2026

---

## Repo Structure

```
urbanride-architecture/
├── README.md
├── docs/
│   ├── 00-executive-summary.md      # written last, by the integrator
│   ├── 01-domain-decomposition.md   # Member 1
│   ├── 02-c4-diagrams.md            # Member 2
│   ├── 03-performance-latency.md    # Member 3
│   ├── 04-scalability-resiliency.md # Member 4
│   ├── 05-infrastructure-cost.md    # Member 5
│   └── 06-qa-defense-notes.md       # shared, everyone adds
├── diagrams/
│   ├── src/                         # .drawio / .mermaid / structurizr source
│   └── export/                      # .png or .svg exports used in the ADD
├── deck/
│   └── urbanride-review.pptx
└── ADD-final.pdf                    # assembled deliverable
```

**Rule that saves you: one member, one file.** Nobody edits someone else's `docs/` file directly. Leave a comment or open an issue instead. This keeps merge conflicts near zero on a one-day turnaround.

---

## Workload Split

| Member | Section | Rubric weight it targets | Deliverable |
|---|---|---|---|
| 1 | Domain & Service Decomposition | 25% | Bounded contexts, service list, database-per-service, Saga pattern for booking→payment, gRPC vs REST vs WebSocket decision table |
| 2 | C4 Diagrams | 15% | Context, Container, and Component diagrams (Component level: Matching Engine) + exports |
| 3 | Performance & Latency | 25% | Kafka topic design for GPS ingestion, Redis Geo / H3 indexing, latency budget table (must sum < 500ms), cache invalidation strategy |
| 4 | Scalability & Resiliency | — (feeds 15% Q&A) | Circuit breakers, rate limiters, backpressure, dead-letter queues, 5x spike walkthrough, CAP trade-off position |
| 5 | Infrastructure & Cost | 20% | Instance sizing rationale, spot/serverless mix, auto-scaling triggers, storage tiering, cost-per-ride calculation |

**Dependency to watch:** Member 2 cannot start until Member 1 publishes the service list. Member 1 should push a rough bullet list of services within the first two hours, then go back and write the detail.

**After individual sections are done:** Member 1 or 2 assembles `ADD-final.pdf`; the other two spare hands build the deck from the finished sections. Everyone contributes three likely panel questions to `06-qa-defense-notes.md`.

---

## Timeline

| When | What |
|---|---|
| Sat, first 2 hrs | Member 1 pushes draft service list. Everyone else sets up their file skeleton. |
| Sat, rest of day | All five write their sections independently. |
| Sat evening | Push everything. Read each other's sections for contradictions (e.g. a service in the diagram that nobody costed). |
| Sun morning | Fix contradictions, finalise numbers, export diagrams. |
| Sun afternoon | Assemble ADD, build deck, dry-run the 10-minute talk. |
| Sun evening | Buffer. Submit before midnight. |

---

## Consistency Checklist

Before submitting, confirm these agree across every section:

- [ ] The same service names appear in the decomposition, the diagrams, and the cost table
- [ ] Latency budget adds up to under 500ms end to end
- [ ] Cost per ride is calculated and stated below 10 LKR
- [ ] Every service has a stated datastore and a stated communication protocol
- [ ] Each diagram is referenced from the text, not just pasted in
- [ ] Deck runs in 10 minutes or less when spoken aloud

---

## Conventions

- Markdown for all docs; diagrams exported to `diagrams/export/` as PNG or SVG
- Branch per member: `feat/<name>-<section>`, PR into `main`
- Keep source files for diagrams so they can be edited later
