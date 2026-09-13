# 3. Communication & Streaming

UrbanRide separates immediate ride requests from continuous GPS traffic and background billing. The request path reads the latest available location index; it never waits for Kafka consumers or payment processing. This supports the **<500 ms transaction target** while absorbing bursts. For planning, the README assumes **150,000 active drivers reporting every 4 seconds: 37,500 GPS events/second**. These figures remain provisional until recorded in `FROZEN.md`.

<!-- Source: ../diagrams/src/03-communication-streaming.mmd, fine-tuned in draw.io and exported to the PNG below. -->
![UrbanRide communication-focused C4 Level 2 container diagram](../diagrams/export/03-communication-streaming.png)

*Figure 3. Solid arrows show direct communication; dashed arrows show Kafka event flow. Service-owned databases are omitted; their design belongs to Member 2.*

## 3.1 Protocol choices

| Link | Protocol | Reason |
|---|---|---|
| Mobile apps → API Gateway | HTTPS/REST | Simple commands and status queries; reuse an idempotency key when retrying ride creation. |
| Driver app → Driver Location Ingestion, through API Gateway | Secure WebSocket (WSS) | A persistent connection carries frequent GPS updates with low connection overhead. |
| Gateway → Trip Management / Identity & Profile; Trip Management → Matching Engine / Surge Pricing | gRPC over TLS | Typed, compact requests for immediate results; reuse connections. |
| Services ↔ Kafka | Kafka protocol over TLS | Independent consumers, buffering and replay decouple GPS, pricing and billing. |
| Billing & Notifications → payment / push providers | HTTPS/REST | Provider integration runs outside the matching request path. |

The gateway validates access tokens locally using cached signing keys; GPS pings do not trigger repeated identity calls. Propagate a bounded request deadline through gRPC calls, with retries only for safe operations within the remaining latency budget. [gRPC deadline guidance](https://grpc.io/docs/guides/deadlines/).

## 3.2 Kafka topics and consumers

| Topic | Partition key / retention | Producer → consumer groups |
|---|---|---|
| `driver.location.v1` | `driverId` / 1 hour | Driver Location Ingestion → `matching-location`, `surge-location` |
| `trip.events.v1` | `tripId` / 7 days | Trip Management → `surge-demand`, `billing-notifications` |
| `billing.events.v1` | `tripId` / 7 days | Billing & Notifications → `trip-billing` |

Start the GPS topic with **48 partitions**: approximately 781 events/second per partition at the assumed load. This is an initial sizing choice, not a measured capacity guarantee. Hashing `driverId` spreads a stadium's drivers across partitions and preserves each driver's log order. Separate consumer groups receive the full stream; replicas within each group share partitions, allowing up to 48 active consumers. GPS retention is short because fresh positions matter more than historical pings, limiting storage cost.

Matching Engine's consumer updates its Redis location index; Surge Pricing independently combines location and trip-demand events. GPS events carry capture time and a per-driver monotonic sequence so replay cannot overwrite a newer position. Events use versioned schemas and unique `eventId` values.

## 3.3 Delivery guarantees

Use replication factor **3**, minimum in-sync replicas **2**, `acks=all` and idempotent producers. Consumers commit offsets after successful processing, giving **at-least-once processing**; replay can repeat business effects unless consumers deduplicate. GPS consumers may discard superseded or expired positions. [Kafka producer guarantees](https://kafka.apache.org/41/configuration/producer-configs/#enable.idempotence), [delivery semantics](https://kafka.apache.org/41/design/design/#message-delivery-semantics).

Trip and billing events must originate from Member 2's transactional outbox. Consumers atomically record processed event IDs with business changes; payment retries reuse the same provider idempotency key. This prevents duplicate effects without claiming end-to-end exactly-once delivery. Owning databases retain authoritative business records beyond Kafka's replay window.

## 3.4 Backpressure and failure handling

- **Protect request capacity.** Isolate stream workers from request handlers. Give GPS connections separate gateway capacity and bounded ingestion buffers. Rate-limit per driver; under overload, retain only the newest unsent position and request slower reporting. On reconnect, send the current position rather than a historical backlog.
- **Keep matching fresh.** Monitor consumer lag in seconds and scale consumers within the partition limit. Use a provisional 2-second lag alert and exclude positions older than 12 seconds. Kafka failure therefore reduces available driver candidates rather than exposing stale positions as current; unpublishable business events remain in the outbox.
- **Recover without retry storms.** Pause affected partitions during dependency outages; retry with bounded exponential backoff and jitter. Quarantine malformed or repeatedly failing individual records in consumer-specific `<topic>.<group>.dlq` topics, retained for 7 days, and alert the owner. Commit the source offset only after durable quarantine. Replay after correction with the original event ID; hold dependent business transitions until the failed event is resolved.
