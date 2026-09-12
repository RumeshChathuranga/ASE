1. **Service names** — taken straight from the brief, so there is nothing to invent:

   | Core services             | Supporting         |
   | ------------------------- | ------------------ |
   | Trip Management           | API Gateway        |
   | Driver Location Ingestion | Identity & Profile |
   | Matching Engine           |                    |
   | Surge Pricing             |                    |
   | Billing & Notifications   |                    |

2. **Diagram tool and template** — generated mermaid code -> fine tuned using draw.io.

3. **Shared numbers** — every section quotes these, so no two sections contradict each other. Suggested starting values, adjust once as a group:

   | Quantity                    | Value                              |
   | --------------------------- | ---------------------------------- |
   | Completed rides/day         | 1,000,000                          |
   | Average ride request rate   | ~12 req/s                          |
   | Peak surge multiplier       | 5x → ~60 req/s                     |
   | Active drivers at peak      | 150,000                            |
   | GPS ping interval           | 4 seconds                          |
   | Resulting ingestion rate    | ~37,500 pings/s                    |
   | Latency budget (end to end) | 500 ms                             |
   | Daily cost ceiling          | 10M LKR (~$33k/day at 300 LKR/USD) |
