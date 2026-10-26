# Decision criteria: Bottle → FastAPI

## Reflections (before we measure)

Picture **700 traders**, each with a live valuation tape open (SSE). The connection hangs for minutes — this is not a one-off “refresh” click.

**Bottle (threads).** Each such connection gets its own worker (a thread). One trader with a stream = one worker sitting and waiting. Seven hundred traders ≈ 700 workers needed. Gunicorn in variant A′ caps out at **200** workers. From client 201 onward, nobody is left to take a new stream (or someone has to be dropped). Those workers barely compute — they mostly wait — but each still takes a slot (memory, pool limit). It is like a call centre with 200 headsets: 700 people on the line will not fit, even if the call is “silence and occasionally one message”.

**FastAPI (async).** One (or a few) workers keep a task list: “hold connection 1…700”. They do not hire 700 people — each connection adds a light “waiting for data” note. When something must be sent, they handle it in turn on the same loop. Seven hundred open streams = **700 sticky notes saying “waiting”**, not 700 full workers.

**Where the advantage is for us.** Not that FastAPI prices faster. That it **holds many idle connections more cheaply and without the artificial 200-thread ceiling**.


|                       | Bottle A′                        | FastAPI              |
| --------------------- | -------------------------------- | -------------------- |
| 200 streams           | OK (pool limit)                  | OK                   |
| 700 streams           | usually **no** (too few threads) | usually **yes**      |
| Memory per connection | more expensive (thread)          | cheaper (small task) |


In one sentence: **Bottle pays one thread per SSE client. FastAPI pays a small task. With 700–1200 traders and open tabs, that decides whether the service can hold them at all — not who computes PnL faster.**

`--threads 200` is an upper pool limit, not “200 CPU cores reserved up front”. SSE threads mostly wait, so CPU can stay low and you still cannot take connection 701. FastAPI wins on live-connection scale and RAM, not on magical CPU reservation.

## What this is for

We check whether our current model (Bottle, one process, **a thread per request and per open SSE**) survives production with **700–1200 concurrent traders** — and whether moving to FastAPI is justified then.

FastAPI helps us mainly with **many long-lived connections** (SSE / later WebSocket, notifications). It does not speed up background valuation work (GIL). It also does not let a stateful service run many processes (cache, queues) — both frameworks stay at **N = 1**.

## Assumption: what a trader looks like

A trader = an active desk user. They may have several browser tabs open (market data, blotter, pricing). The frontend opens a separate `EventSource` per view (`sseClient.ts`) — **each tab = a separate SSE**.


| What we count            | Value                   | Why                                                                    |
| ------------------------ | ----------------------- | ---------------------------------------------------------------------- |
| Concurrent traders       | 700–1200                | production target                                                      |
| SSE per trader           | 2 on average, 3 at peak | valuation-stream + market-data `/stream` (+ third tab / notifications) |
| Peak SSE per service     | 1400–3600               | 700×2 … 1200×3                                                         |
| Bottle thread limit (A′) | **200**                 | `gunicorn -k gthread -w 1 --threads 200`                               |
| Short HTTP from UI       | ~600 req/s              | 1200 traders × `/status` every 2 s                                     |
| Generator HTTP           | ~30 req/s               | at this scale the UI dominates anyway                                  |


Conclusion without measuring: **200 threads < 700 SSE**. A′ alone cannot hold the lower trader bound on streams while 1 SSE = 1 thread. We confirm that with measurement (RSS, whether `/health` still answers).

Today the pricing cache has an accidental limit of 50 trades (`get_trades` → `limit=50`). In production we mark all open positions — that is a separate CPU/logic problem (thesis T6), not an argument “therefore FastAPI”.

## What we compare

All variants: **1 process**.


| Variant      | How we run it                            | Role                              |
| ------------ | ---------------------------------------- | --------------------------------- |
| A            | `gunicorn -w 1`                          | books-service today (1 thread)    |
| A′           | `gunicorn -k gthread -w 1 --threads 200` | rest of the system — **baseline** |
| B            | `uvicorn --workers 1`, `async def`       | candidate                         |
| C (optional) | `uvicorn --workers 1`, `def`             | FastAPI without asyncio on I/O    |


Decision = **B versus A′**.

Scenarios:


| Id  | Endpoint           | What it stands for here                |
| --- | ------------------ | -------------------------------------- |
| S1  | `/health`          | framework overhead; probe under SSE    |
| S2  | `/io` (50 ms stub) | waiting on another service             |
| S3  | `/cpu`             | compute (barrier: async does not help) |
| S4  | `/db`              | typical DB route                       |
| S5  | long-lived SSE     | like `/valuation-stream` / `/stream`   |


S1–S4: `oha`. S5: a script that holds N connections plus a parallel `/health` probe (`oha` does not hold SSE).

## Two pass bars

Response timeout: **2 s** (shortest in the system). Errors: **≤ 1%**. “Clearly” = min–max ranges from repeats do not overlap (≥ 3 runs, target 6).

### W_sse (scenario S5) — main production bar

A variant passes if:

1. it holds **≥ 700** concurrent streams (lower bound); target **≥ 1200**;
2. dropped connections ≤ 1%;
3. at the same time `/health`: p99 < 2 s, errors ≤ 1%.

Grid: c = 200 (A′ ceiling), 700, 1200. We also record **RSS**.

### W_http (scenarios S1–S4)

A variant passes under UI traffic if:

1. **≥ 1200 req/s** (2 × ~600 — headroom at 50% utilisation of one queue);
2. p99 < 2 s;
3. errors ≤ 1%.

Grid: c ∈ {1, 10, 50, 200, 700} where feasible.

We do not use “p95 −30% or throughput ×2” as a separate bar — with a fixed client count that is the same thing (Little’s law). The question is: **does A′ carry 700–1200 traders**, not “how many percent faster is FastAPI”.

## Theses

**T1 — long-lived SSE (S5) — main GO reason**

- **Rule:** We move to FastAPI (at least for streaming services) when **A′ (Bottle)** fails W_sse at 700+ and **B (FastAPI)** passes.
- **Why:** **Bottle:** 1 SSE client = 1 thread (~MB RAM) for the whole lifetime. At hundreds/thousands of tabs the 200 pool breaks; HTTP stalls while CPU is idle. **FastAPI:** light tasks on the loop. This is the “thread vs event loop” picture. Future WebSocket / notifications are the same class.

**T2 — short I/O (S2)**

- **Rule:** Supports GO when **A′ (Bottle)** fails W_http on S2 and **B (FastAPI,** `async def`**)** passes.
- **Why:** ~600 req/s from the UI plus waiting on other services.

**T3 — database (S4)**

- **Rule:** Same as T2 for S4: **A′ (Bottle)** fails W_http, **B (FastAPI, async DB)** passes, plus no connection-pool errors.
- **Why:** Most business routes. The DB pool waits up to 30 s; we time out at 2 s.

**T4 — CPU (S3) — barrier against migration**

- **Rule:** Against GO if **B (FastAPI,** `def` **on** `/cpu`**)** is clearly slower than **A′ (Bottle)** and falls below W_http.
- **Why:** Compute still runs on one core. **FastAPI** does not “win” here.

**T5 —** `/health` **overhead (S1) — barrier**

- **Rule:** Against GO if **B (FastAPI)** is clearly slower than **A′ (Bottle)** and falls below W_http (or breaks the probe under S5).
- **Why:** Blatant framework regression on the **FastAPI** side.

**T6 — pricing and growing data — not a reason for FastAPI**

- **Rule:** Removing the cache limit of 50 and a larger valuation flow **do not** justify migration. First in **Bottle:** index by symbol, **one SSE frame per tick**, less logging. Only if **A′ (Bottle)** still cannot hold 700–1200 streams after that → back to T1 (**FastAPI** only if connections are the problem).
- **Why:** At a 100 ms tick and marking every open position on a symbol, you easily reach thousands–tens of thousands of valuations/s. With 1200 SSE clients each valuation × 1200 sends. That is background CPU and fan-out — the same in **Bottle** and **FastAPI**. The framework does not add cores.

**R1 test (async safety, not a GO thesis)**

In **B (FastAPI):** `/cpu-async` (c=50) + `/health` (c=1). p99 of `/health` = how hard blocking code freezes the **FastAPI** event loop → [risk_matrix.md](risk_matrix.md).

**When to revisit the decision (no new measurement now)**

- WebSocket, push, new fan-out services, many more frames per stream → T1.
- Many new routes / HTTP clients over years → maintenance cost (Pydantic, `/docs`), not throughput alone.
- Provider quotas, a second process “just in case” → FastAPI does not fix that.



## How we decide


| Outcome        | When                                                                                                                                            |
| -------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| **GO**         | T1 yes; T4 and T5 not against; high/high risks from the matrix have mitigations                                                                 |
| **Partial GO** | T1 yes → migrate pricing / market-data / monitoring (streams). Rest stays on Bottle if T2/T3 fail                                               |
| **NO-GO**      | T1 no **or** T4/T5 against → fix pricing first + start under Gunicorn; revisit on WebSocket / new fan-outs or when T2/T3 under W_http defeat A′ |


If C ≈ A′ on S1–S4: “FastAPI with `def`” alone buys nothing on short HTTP — T1 remains.

## What we expect before measuring

- **T1:** A′ fails at 700+ SSE; B passes with lower RSS → GO or partial GO.
- **T2 / T3:** unknown at 1200 req/s; short HTTP may still live on A′ while only SSE dies.
- **T4 / T5:** unlikely against.
- **T6:** regardless of T1 — pricing cleanup is needed anyway.

