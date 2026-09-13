# 02 - Data Design & Transactions

## Database-per-Service Strategy

We're strictly isolating datastores for each microservice. This prevents noisy neighbors (e.g., heavy GPS writes impacting billing reads) and lets us tune databases specifically for cost and latency.

| Service | Datastore | Rationale & Justification | CAP Stance |
| :--- | :--- | :--- | :--- |
| **Trip Management** | PostgreSQL (Aurora Serverless) | We need strong ACID guarantees here. A serverless SQL setup scales up during the 5x surge and scales down to save costs when traffic drops. | CP |
| **Driver Location Ingestion** | Kafka (Buffer) + Redis (Geo) + S3 (Cold) | Dumping 37,500 pings/s straight into a DB is too expensive. Phones ping Kafka first. A worker consumes the stream to keep the Redis Geo-index updated for fast matching. Raw historical data goes straight to S3 for cheap archiving. | AP |
| **Matching Engine** | Redis (Cache) | Fully stateless. It just reads driver caches and ride requests. We use ephemeral storage because we can easily rebuild the cache if it drops. | AP |
| **Surge Pricing** | DynamoDB (Serverless) | Needs fast reads/writes for multipliers. DynamoDB handles the burst traffic well and we only pay for what we use. | AP |
| **Billing & Notifications** | PostgreSQL (Serverless) | Financial data has to be strongly consistent (no double charges). We're using the Outbox pattern here to decouple the actual payment transaction from sending the slower SMS notifications. | CP |

---

## Saga Pattern (Distributed Transactions)

![Saga Sequence Diagram](../diagrams/export/02-data-transactions.png)

We can't use 2PC (Two-Phase Commit) across microservices, so we use a **Hybrid Saga Pattern**. 
* **Phase 1 (Ride Booking):** Uses **Orchestration** (Trip Management synchronously commands Surge Pricing and Billing via APIs) because we must immediately reject riders if their card has no funds.
* **Phase 2 (Ride Completion):** Uses **Choreography** (fully decentralized and event-driven via Kafka) because it must be highly available and decoupled during massive drop-off surges. 

### 1. Successful Transaction Flow
* **Trip Management** marks ride as `COMPLETED` -> fires `RideCompleted` event.
* **Parallel Event Consumption:**
  * **Surge Pricing** consumes the event to update its internal supply/demand metrics (since the driver is now free).
  * **Billing** consumes the event, relies on the *upfront locked price* agreed upon at booking, and hits the payment gateway to **capture** the previously authorized hold -> fires `PaymentSucceeded`.

### 2. Failure Scenario & Compensating Actions
If the credit card declines *after* the ride is over, we obviously can't rollback the physical ride.
* **Billing** gets a decline from the gateway and fires a `PaymentFailed` event.
* **Compensating Action (Billing):** We record a debt on the user's account and block them from requesting new rides.
* **Compensating Action (Trip Management):** Listens for the failure and updates the ride state to `PAYMENT_PENDING`.
* **Notifications:** Sends an SMS telling the user to update their card.

---

## Idempotency

Double-charging a rider is the worst-case scenario. We enforce idempotency on all API endpoints and event consumers. 
* Every request/event includes a unique `Idempotency-Key` (UUID).
* The receiving service checks an `idempotency_keys` table (or Redis cache).
* If we've seen the key before, we just return the cached success response instead of running the logic again. This protects us if Kafka accidentally delivers the same message twice during a network blip.

---

## Transactional Outbox

We have a dual-write problem: if a service updates its database and then publishes to Kafka, it could crash in between and drop the event.

We fix this with the **Outbox Pattern**:
1. When Trip Management updates a ride, it also inserts the event payload into a local `outbox` table in the *same DB transaction*.
2. We use a **Change Data Capture (CDC) tool like Debezium**. Instead of running generic background queries that cause database lock contention, Debezium reads the database's Write-Ahead Log (WAL) at the file level and pushes the event to Kafka.
3. The process waits for an "ACK" (an acknowledgement from Kafka that the message was safely saved). If it doesn't get this ACK (e.g., if the network drops), it keeps trying to send it. This guarantees the message is delivered **at least once** and is never lost during a crash.

---

## Handling Lock Contention (Double Booking)

During a 5x surge, multiple riders in the same area might try to match with the same driver at the exact same time.

Using pessimistic database locks would destroy our 500ms latency budget. Instead, we use **Optimistic Locking in Redis**.
* When the Matching Engine finds a driver, it runs a quick Lua script in Redis.
* The script atomically checks if `status == AVAILABLE` and flips it to `ASSIGNED`.
* If it returns `0` (meaning another thread already grabbed them a millisecond ago), our thread just moves on to the next driver in the queue. No waiting, no DB deadlocks.
