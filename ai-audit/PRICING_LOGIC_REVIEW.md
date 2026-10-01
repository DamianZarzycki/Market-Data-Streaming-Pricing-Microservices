# Pricing logic review — provider-aware repricing, tonight's changes

**Scope:** self-review of the provider-aware pricing work added this session
(`pricing-service/calculation_service.py::price_trade_from_provider_quote`,
`pricing-service/worker.py::provider_pricing_worker`, and how it relates to
`trade-action-service`'s CLOSE_TRADE handler) against how this should behave
in a real trading application, using the same rigor applied to the
CLOSE_TRADE bid/ask/validation fixes made earlier tonight.

**Date:** 2026-09-19

**Bottom line:** the close-side fixes (side-aware price, validated fallback,
rollback-before-commit) are correct. The *open-position* (unrealized) marking
introduced alongside it has the same class of bug the close-side fix just
corrected — it just hasn't bitten yet because nobody compared the two paths
side by side. Two more real gaps below (stream, staleness) are more
architectural and need a decision, not just a patch.

---

## 1. HIGH — Unrealized marking is not side-aware, and prefers `last` over mid

`calculation_service.price_trade_from_provider_quote()`:

```python
if last is not None:
    current_price = float(last)
elif bid is not None and ask is not None:
    current_price = (float(bid) + float(ask)) / 2.0
```

Two problems, both real:

- **No side-awareness.** The CLOSE_TRADE fix earlier tonight established the
  right principle: closing a long *sells into the bid*, closing a short
  *buys back at the ask* — using the midpoint gives away the spread. The
  exact same principle applies to **marking an open position**: a real desk
  values a long at the bid and a short at the ask (the price you could
  *actually* exit at right now), not at mid. Right now this function uses
  one `current_price` for both sides, so a long and a short in the same
  symbol at the same quote get marked identically — and inconsistently with
  how their eventual close will be priced.
- **`last` is preferred over mid even when a live two-sided quote exists.**
  `last` is a trade print — it can be arbitrarily stale (the mock Finnhub
  data we tested against tonight was hours old). If `bid`/`ask` are both
  present, that's a *live, current* quote and should take priority; `last`
  should be the fallback, not the other way round.

**Concrete symptom:** the same trade can show an inflated unrealized gain
while open (marked favorably), then a smaller realized gain — or a loss —
the instant it's closed, purely because the two code paths use different
marking conventions. That's not a market move, it's an internal
inconsistency.

**Fix:** mirror the CLOSE_TRADE logic exactly —
`is_long → bid (fallback last)`, `is_short → ask (fallback last)`. Never
average bid/ask for either open or close.

---

## 2. HIGH — Closing a trade never reaches the live valuation stream

`trade-action-service/worker.py`'s CLOSE_TRADE branch writes
`closing_valuation` straight to the DB (`db.valuations.add(...)`). It never
touches `pricing-service`'s `calculation_service.store_and_publish()` /
`sse_subscribers` — that state (the SSE subscriber queues) lives entirely in
a **different process's memory**. There is no bridge between the two.

Consequence: a client subscribed to `pricing-service`'s
`GET /valuation-stream` will see live unrealized-PnL ticks while a position
is open, but will **never see the realized-PnL event when it closes** — the
close only shows up if that client separately polls
`GET /valuations`/`GET /valuations/<trade_id>` afterward.

The requirement says the stream should carry realized PnL. Right now it only
does for the (currently unused) case where pricing-service itself would
close something — which it doesn't; trade-action-service owns CLOSE_TRADE
entirely.

**This needs a decision, not just a patch — options:**
- trade-action-service calls a small pricing-service endpoint (or drops onto
  a shared queue/DB LISTEN-NOTIFY) to push the closing valuation into the
  same `sse_subscribers` fan-out, or
- pricing-service owns writing *all* valuations (open and close), and
  trade-action-service only computes/validates the close price and hands it
  off, or
- accept the gap and document that realized PnL is DB-only, stream-on-open-only.

## 3. HIGH — No staleness gate on the marks `provider_pricing_worker` uses

`trade-action-service`'s OPEN_TRADE enforces `check_provider_data_staleness`
with per-asset-class age windows before letting a trade open at all. Once
open, `provider_pricing_worker` re-marks it every 5s using whatever
`GET /market-data/quotes/<provider>/<symbol>` returns — with **zero age
check**. If that provider stops updating (rate-limited — which we already
saw happen tonight with Alpha Vantage/Twelve Data — market closed, upstream
outage), the position keeps silently re-publishing the same stale price
forever, indistinguishable from a live mark. There's no "STALE" signal
anywhere in the valuation payload or stream.

**Fix:** either skip marking (and flag it, e.g. `last_pricing_time` frozen)
when the quote is older than the same per-asset-class window used at open,
or at minimum carry an `is_stale`/`age_seconds` field in `valuation_data` so
downstream consumers (UI, risk) can tell.

---

## 4. MEDIUM — Polling instead of streaming (deliberate tradeoff, flagging for a second look)

`provider_pricing_worker` polls REST every 5s per (provider, symbol) pair
rather than subscribing to `market-data-service-integration`'s own
`/market-data/stream`. This was a deliberate choice tonight to avoid adding
a second persistent SSE client inside pricing-service (mirroring the
existing `pricing_worker` pattern would have been the "purer" architecture).
Consequence: up to 5s of latency, and N extra HTTP round trips per cycle
instead of push-based updates. Worth revisiting if sub-5s latency actually
matters for this system; not wrong, just a simplicity/latency tradeoff made
under time pressure.

## 5. MEDIUM — `float`, not `Decimal`, throughout PnL math

Pre-existing across the *entire* codebase (`calculate_pnl`,
`Trade.trade_price`, everything gets `float()`-cast before arithmetic) — not
introduced tonight, but worth naming in a "how should this really look"
review since it's a textbook real-money correctness issue (floating-point
drift accumulates over many trades/valuations). Fixing it properly is a
codebase-wide change (`Numeric`/`Decimal` end to end), well beyond tonight's
scope — flagging so it doesn't get lost.

---

## Confirmed correct / aligned with how this should work

- **Provider isolation** is enforced correctly: `recalculate_valuations()`'s
  `and not t.market_data_provider` guard means a provider-assigned trade is
  *never* repriced by the simulator path, and `provider_pricing_worker` only
  ever fetches that one assigned provider's quote for that trade — no
  mixing of sources, per the original requirement.
- **CLOSE_TRADE** (after tonight's fix) correctly re-derives price from the
  trade's own assigned provider rather than trusting caller input, is
  side-aware (bid for closing long, ask for closing short, matching real
  execution), validates the non-provider fallback price instead of raising,
  and rolls back before the audit-only commit on rejection.
- **Schema** (`Valuation.market_data_provider`, `market_data_timestamp`, and
  `market_data_snapshots` now getting real `QUOTE`/`CURVE` rows from
  `market-data-service-integration`) matches the stated data-model
  requirements.

## Suggested priority for tomorrow

1. **#1** — make unrealized marking side-aware + prefer bid/ask over `last`.
   Small, contained, same function already touched tonight.
2. **#3** — add a staleness signal to `price_trade_from_provider_quote`.
   Small addition, reuses the age-window concept already built for OPEN_TRADE.
3. **#2** — needs an architecture decision before coding (see options above).
4. **#4** — revisit only if latency in practice turns out to matter.
5. **#5** — separate, larger effort; not urgent, just don't forget it exists.
