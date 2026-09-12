UrbanRide — Architecture Design Document
Section 1: Context & Decomposition

Table of Contents

1.1 Scale Assumptions
1.2 System Context (C4 Level 1)
1.3 Domain & Service Decomposition
1.4 Data Ownership Summary
1.5 What Was Deliberately Not Split — and Why
1.6 Why Not a Monolith?
1.7 CAP Stance Per Service
1.8 Cost Traceability
1.9 Diagram Deliverable
1.10 Summary

1.1 Scale Assumptions

UrbanRide is designed around the following system-scale assumptions, agreed by the team in FROZEN.md:

Metric	Target
Completed rides/day	1,000,000
Average ride request rate	~12 req/s
Peak ride request rate (5× surge)	~60 req/s
Active drivers at peak	150,000
GPS ping interval	4 seconds
Driver location ingestion rate	~37,500 pings/s
End-to-end latency budget	< 500 ms
Infrastructure cost ceiling	< 10 LKR/ride (≈10M LKR/day)

Location ingestion (~37,500/s) runs at roughly 625× the peak ride-request rate (~60/s) — the single largest driver of every decomposition decision in this section.

1.2 System Context (C4 Level 1)

UrbanRide is treated as one system from the outside, with two human actors and three external dependencies:

External Party	Relationship to UrbanRide
Rider (mobile app)	Requests rides, pays, rates trips
Driver (mobile app)	Sends GPS pings, accepts/rejects matches, updates trip status
Payment Gateway	Card/wallet settlement
Maps / Routing Provider	ETA and route calculation
SMS / Push Notification Provider	Trip and OTP delivery


1.3 Domain & Service Decomposition

1.3.1 Boundary Principle

Boundaries are drawn where workload characteristics diverge or consistency requirements diverge — not by arbitrary technical layering, and not simply "one microservice per noun in the brief."

Concretely: a capability gets its own service when it needs to scale independently, fail independently, or hold a different CAP stance than its neighbors. Otherwise it stays merged (see Section 1.5).

1.3.2 Bounded Contexts

#	Service	Bounded Context / Responsibility	Primary Data Store	Why It's Separate
1	API Gateway	Request routing, auth-token validation, edge rate limiting	Routing config + rate-limit counters (no business data)	Only component exposed to the internet; different scaling and security posture than every internal service
2	Identity & Profile	User/driver identity, credentials, KYC/verification state	PostgreSQL (Section 2, Member 2)	Auth data needs strict, durable consistency with a lifecycle entirely distinct from operational trip data
3	Trip Management	Trip lifecycle state machine — requested → matched → in-progress → completed/cancelled	PostgreSQL, Aurora Serverless (Section 2)	Single source of truth for trip state; the only service permitted to declare a trip's status
4	Driver Location Ingestion	Accept, validate, and publish driver GPS pings at ~37,500/s peak	Kafka buffer → Redis Geo index → S3 cold archive (Section 2)	Highest-throughput path in the platform (~625× ride-request rate); must scale independently of every transactional service
5	Matching Engine	Candidate search, scoring, and driver reservation	Redis (reservations), reads via gRPC (Section 2)	The system's core low-latency algorithm; CPU-bound, independently tunable, with one narrow CP lock for the assignment write
6	Surge Pricing	Zone-level demand/supply signal aggregation, active multiplier calculation	DynamoDB (Section 2)	A streaming-analytics workload with its own update cadence, decoupled from individual match or billing decisions
7	Billing & Notifications	Fare capture, payment processing, trip/OTP notification delivery	PostgreSQL, Outbox pattern (Section 2)	Financial-correctness requirements are incompatible with the availability posture of the dispatch path


1.4 Data Ownership Summary

No service directly accesses or modifies another service's database — cross-service access happens via API call or event only.

Service	Owns
API Gateway	Rate-limit counters, routing config
Identity & Profile	User/driver profiles, credentials, KYC state
Trip Management	Trip records, trip state machine, audit log
Driver Location Ingestion	Current + recent driver GPS pings
Matching Engine	Candidate sets, match decisions, match-attempt log
Surge Pricing	Zone-level demand/supply signals, active multiplier
Billing & Notifications	Fare ledger, payment status, notification log

Examples: Matching Engine reads location data but doesn't own it; Trip Management requests a match but Matching Engine owns the decision; Billing consumes trip-completion events but Trip Management owns trip state.

1.5 What Was Deliberately Not Split — and Why
Considered Split	Why Rejected
Separate Billing vs. Notifications services	Both are low-volume, both trigger off the same trip-completion event — splitting adds a hop for no scaling benefit
Standalone Fare Calculation service	Pure function of trip data + surge multiplier, no independent scaling profile — would add a 4th network call to the payment Saga
Per-lifecycle-stage trip services	Stages of one state machine needing a single authoritative owner
Merge Surge Pricing into Matching Engine	Different data shape (aggregated, slow-updating) and different scaling driver (geo-cell count, not request rate)
Per-city / per-region service instances	Regional variation is a data-partitioning and deployment concern (Section 5, Member 5), not a domain boundary
Separate API Gateway per client type	Splitting duplicates auth/rate-limiting logic without reducing coupling
1.6 Why Not a Monolith?

A monolith combining ride transactions, GPS ingestion, matching, pricing, billing, and notifications would let the ~37,500 pings/s location workload force the entire application to scale together — directly threatening the 10 LKR/ride cost ceiling — and would let a slowdown in one capability cascade into unrelated ones (e.g., a Matching Engine spike corrupting completed-trip billing).

The 7-service decomposition instead gives: independent scaling, fault isolation, clear data ownership, independent evolution, and workload isolation — without fragmenting further than Section 1.3.1's boundary principle justifies.

1.7 CAP Stance Per Service
Service	Stance	Why
Driver Location Ingestion	AP	At ~37,500 pings/s, a consensus write per ping blows the latency budget. Staleness of a few seconds is harmless — corrected by the next ping.
Matching Engine	AP-leaning, narrow CP lock	Candidate search is AP; the final driver-assignment write is a short CP operation to prevent double-booking.
Surge Pricing	AP	A cached, periodically-refreshed multiplier — staleness is cheaper than blocking the match path.
Trip Management	CP	The trip state machine must not fork — reduced availability beats corrupted trip state.
Billing & Notifications	CP (billing) / AP (notifications)	Money can't be eventually consistent; a delayed push notification has no financial consequence.
Identity & Profile	CP	Auth/role data must be immediately consistent — a revoked driver must not keep matching.

The two decisions the panel will probe hardest: Driver Location Ingestion → AP, Billing → CP.

1.8 Cost Traceability

1,000,000 rides/day × 10 LKR ceiling = 10,000,000 LKR/day, matching FROZEN.md exactly. The decomposition supports this by letting each service scale to its own actual workload (Section 5's sizing) instead of provisioning every service for the highest-volume component.

1.9 Diagram Deliverable

C4 Level 1 — System Context
Source: diagrams/src/context-diagram.mmd · Export: diagrams/export/context-diagram.svg

Must show: Rider, Driver, UrbanRide, Payment Gateway, Maps/Routing Provider, SMS/Push Notification Provider.
Must NOT show: any of the 7 internal services, internal databases, Kafka topics, Redis, or API details — those belong to Level 2/3.

(Diagram image below)

1.10 Summary

UrbanRide decomposes into 7 services under one boundary principle: split where workload or consistency diverges, merge where it doesn't. The ~625× gap between location ingestion (~37,500/s) and ride requests (~60/s) is the dominant driver; CAP stance is assigned per service (Location → AP, Billing → CP) rather than applied uniformly. This supports 1,000,000 rides/day, <500ms latency, 5× surge absorption, and <10 LKR/ride — while keeping every boundary defensible under panel questioning rather than decorative.
