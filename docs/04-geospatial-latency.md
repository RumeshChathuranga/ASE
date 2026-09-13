# 04 - Geospatial Matching & Latency

This section covers the part of UrbanRide that must be fastest: finding a nearby free driver for a rider. It explains which geo-index we use and why, shows a step-by-step latency budget that stays under the 500 ms target, describes how the driver index is kept correct when drivers change state, and lists the protections (rate limiter, circuit breaker, atomic lock) that keep matching working during a 5x surge. All numbers come from `FROZEN.md`. The Matching Engine's own component diagram (C4 Level 3) is at the end.

## Geo-Index Choice: Redis Geo vs H3 vs S2

The Matching Engine needs to find nearby drivers fast, at 60 req/s peak, with 150,000 active drivers pinging every 4 seconds (~37,500 pings/s).

| Option | How it works | Verdict |
| :--- | :--- | :--- |
| **Redis Geo** | Sorted set using geohash, `GEOSEARCH` finds drivers within a radius in-memory | **Chosen.** Sub-millisecond radius search, driver locations already live in Redis from ingestion, no extra moving parts |
| Uber H3 | Splits the map into hexagon cells, drivers looked up by cell ID | Great for uneven density (rural vs city), but adds a second index to keep in sync with Redis. Not needed at our scale |
| Google S2 | Splits the map into a hierarchy of cells using space-filling curves | Very precise, but heavier to compute and mainly useful for global-scale spatial joins we don't need here |

**Layout:** one Redis Geo key per city/region (e.g. `drivers:geo:colombo`), so a lookup only ever scans drivers in that region instead of the whole fleet. Each driver's status (`AVAILABLE` / `ASSIGNED`) is stored alongside as a normal Redis key, checked right after the geo lookup.

| Key | Type | Holds | TTL |
| :--- | :--- | :--- | :--- |
| `drivers:geo:<city>` | Geo set | Latest position of every active driver in that city | Entries refreshed on every ping |
| `driver:<id>:status` | String | `AVAILABLE` or `ASSIGNED` | ~15 s, refreshed on every ping |

---

## Handling 37,500 GPS Writes per Second

Reads (finding drivers) are only ~60 per second at peak. Writes (driver pings) are ~37,500 per second. So the index is write-heavy, and the design must make writes cheap.

- GPS pings do **not** hit the Matching Engine directly. Driver Location Ingestion publishes them to the Kafka topic `driver.location.v1` (Member 3's design). The Matching Engine's **Location Consumer** reads that topic and writes to Redis. If Redis is briefly slow, pings queue up in Kafka instead of being lost.
- The consumer writes in **batches** using Redis pipelining (many commands in one network round-trip). One `GEOADD` plus one `SET ... EX 15` per ping is about 75,000 simple commands per second. A single Redis node handles well over 100,000 per second, and the index is split across 3 shards by city, so there is headroom for the 5x surge.
- The data is small. 150,000 drivers × roughly 100 bytes each is about **15 MB** — it fits in memory many times over. Sizing and cost of the Redis cluster are in Member 5's section.
- Because every driver pings every 4 seconds, the index **rebuilds itself in about 4 seconds** from the live stream. We do not need to persist it to disk or restore it from backup after a restart.

---

## Latency Budget (must stay under 500ms)

| Step | Where it happens | Budget |
| :--- | :--- | :--- |
| Rider app → API Gateway | Network + TLS | 30 ms |
| API Gateway → Matching Engine | Internal call (gRPC) | 10 ms |
| Find nearby drivers | Redis `GEOSEARCH` | 15 ms |
| Filter & rank candidates | Matching Engine logic | 25 ms |
| Lock the chosen driver | Redis Lua script (atomic flip, see below) | 10 ms |
| Confirm fare | Call to Surge Pricing (gRPC) | 40 ms |
| Publish match event | Kafka producer (async, doesn't block the reply) | 5 ms |
| Response → Rider app | Network | 30 ms |
| **Subtotal** | | **165 ms** |
| Safety margin (queueing, retries, surge slowdown) | | 335 ms |
| **Total** | | **500 ms** |

Keeping the "happy path" around 165ms leaves a big margin for the 5x surge, when queues get longer and downstream calls are slower.

---

## Cache Invalidation on Driver State Change

The Redis Geo index has to always reflect who is actually free, or the engine will keep offering drivers who are already on a ride.

- Every driver state change (`AVAILABLE`, `ASSIGNED`, `OFFLINE`) is written straight to Redis the moment it happens — there is no batch job, so there is no stale window.
- A driver's location and status share the same TTL (~15 seconds). If a driver's phone stops sending pings (crash, tunnel, dead battery), the entry expires on its own and drops out of matching instead of staying "available" forever.
- The atomic lock described below (optimistic locking in Redis) is what stops two riders from being matched to the same driver in the gap between "found" and "assigned."

---

## Atomic Driver Lock (no double-booking)

Two riders can ask for a driver at the same moment and both `GEOSEARCH` calls can return the same driver. To make sure only one of them gets the driver, the "check and assign" step runs as **one Redis Lua script**. Redis runs a script without letting any other command in between, so the check and the write cannot be split.

```lua
-- KEYS[1] = driver:<id>:status   ARGV[1] = trip id
if redis.call('GET', KEYS[1]) == 'AVAILABLE' then
  redis.call('SET', KEYS[1], 'ASSIGNED:' .. ARGV[1], 'EX', 15)
  return 1   -- locked, this rider gets the driver
end
return 0     -- someone else got there first
```

- If the script returns `0`, the Matching Engine simply tries the **next driver in its ranked list**. It does not fail the whole request. This is why "filter & rank" returns a short list of candidates, not just one.
- The lock carries the trip id, so a later "release" only clears the lock if it still belongs to that trip.
- The lock has the same ~15 s TTL. If the Matching Engine crashes right after locking, the lock expires on its own and the driver becomes available again. Trip Management then confirms the assignment in its own database (Member 2's Saga), which is the long-term record — Redis only holds the short-lived lock.

---

## Protecting the Matching Path: Circuit Breakers & Rate Limiters

During a 5x surge the Matching Engine gets hit hardest, so it needs to fail safely instead of falling over.

- **Rate limiter** on incoming match requests at the API Gateway. Once the Matching Engine is at capacity, extra requests are queued briefly or get a "high demand, please wait" response, instead of overloading Redis.
- **Circuit breaker** on the call to Surge Pricing. If that service starts timing out, the breaker trips and the Matching Engine falls back to the last known price/multiplier instead of blocking every match request on a slow dependency.
- **Bulkheads**: the Matching Engine only talks to Redis and Surge Pricing directly. It never calls Billing or Notifications, so a slowdown in those services can't spread into the matching path.
- If Redis itself is unreachable, the engine returns "no drivers found" rather than hanging — riders can retry, which is better than every request queuing up and breaching the 500ms budget for everyone.

---

## When Redis Fails or the Network Splits

The location index is deliberately **AP** (available over consistent) in CAP terms. A driver position that is a few seconds old is fine; a matching service that is down is not.

- **One Redis node dies:** each shard has a replica. Failover takes a few seconds. During that gap, matching returns "no drivers found" for that city and riders retry. Nothing is lost, because the pings keep arriving through Kafka and refill the new primary within ~4 seconds.
- **Network split between the Matching Engine and Redis:** the engine treats it the same as Redis being down — fast failure, no hanging. The Kafka consumer pauses and its offset does not move, so pings are replayed once the link is back.
- **Network split between regions:** it does not matter. A rider in Colombo is only ever matched with drivers in Colombo, so the index is never shared across regions (see Member 5's region cells).
- **Stale driver after a split:** a driver who was `AVAILABLE` before the split but is now offline will time out from the index in ~15 seconds. The worst case is one failed match attempt against that driver, which the atomic lock and the "try the next candidate" rule already handle.

The only thing that must be **CP** (consistent) is the final "this driver is on this trip" record. That lives in Trip Management's database and is protected by the Saga (Member 2), not in Redis.

---

## Diagram: C4 Level 3 Component View — Matching Engine

<!-- Source: ../diagrams/src/04-geospatial-latency.mmd. Regenerate with: mmdc -i diagrams/src/04-geospatial-latency.mmd -o diagrams/export/04-geospatial-latency.png -b white -w 1600 -s 2 -->
![UrbanRide Matching Engine C4 Level 3 component diagram](../diagrams/export/04-geospatial-latency.png)

*Figure 4. The parts inside the Matching Engine. Solid arrows are direct calls on the matching path, numbered 1–5 in the order the Match Request Handler runs them, with the time budget for each step from the table above. Dashed arrows are Kafka events and never block a match: the Location Consumer keeps the Redis index fresh from `driver.location.v1`, and the Match Event Publisher sends `MatchFound` without waiting for a reply. The circuit breaker on Surge Pricing lives in the Fare Confirmer; the rate limiter sits in front of the engine, at the API Gateway.*

