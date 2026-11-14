# 2026 World Cup: live results and post-mortem

Live order book, real orders, June 27 to July 18, 2026.

## Headline

| Metric | Value |
|---|---|
| Live orders filled | 90+, across 50+ in-play markets |
| Order-book updates processed | 100K+ |
| Outcome | Net profitable: **+13.4% return on capital deployed** |
| Fees | about 4% of capital deployed |

Every figure comes from `ledger.py --live`. It rebuilds each position from the
system's own SQLite log and reconciles it against the exchange's fill and
settlement records.

## By strategy ("lane")

| Lane | Positions | Win rate | Return on capital deployed |
|---|---|---|---|
| Edge: enter when the model's fair value beats the price | 31 | 52% | +12.9% |
| Reaction: enter at fair value, exit into goal-driven price spikes | 27 | 56% | +15.2% |

**Read these honestly.** One trade produced about 70% of the edge lane's profit;
without it, that lane is negative. The reaction lane worked as designed every time
it fired, but 27 positions is too few to call it a proven edge. The pre-registered
bar was 100 live trades. The system was net profitable and never had a catastrophic
day after the risk layer was rebuilt, but it did **not** statistically prove an edge.

## What the safety process caught

Every new strategy ran in "shadow" first: logged as if traded, priced at the depth it
would really fill at, with nothing at risk. Arming thresholds were written down
before seeing results.

- **Four ideas were rejected before risking money:** a model-repricing sniper,
  resting maker orders, a paid sharp-odds feed, and a directional-asymmetry rule.
- **One approval was wrong, and the audit caught it.** A "buy markets that are already
  mathematically decided" sniper passed its gate in shadow, went live, and filled 0 of
  14 orders. Logging the exchange's own order book next to ours showed why: our local
  copy showed asks of 13–42¢ while the exchange sat at 99¢. The book had gone stale
  **without any gap in sequence numbers**, so the standard check never fired. Nothing
  was lost, because none of those orders filled.

## Bugs found by auditing logs against the exchange

The nine fixes are listed in the [README](../README.md#audit-9-bugs-found-and-fixed),
each with a regression test (`tests/test_audit.py` and the per-feature suites). The
two that inflated reported P&L:

- **Double-counted exits.** A position sold before settlement was also credited its
  settlement payout. The ledger now rebuilds every position from exchange fills,
  splitting sold quantity from quantity held to settlement.
- **Stale fills.** Simulated fills priced at the mid overstated strategy P&L. They are
  now priced at the worst order-book level needed to fill the whole order.

## Two takeaways

1. **Money came from reacting to verified events, not from prediction.** Profit came
   from "the score changed and the price hadn't caught up yet." Model-predicted
   entries roughly broke even.
2. **Measurement mattered more than strategy.** The worst bugs of the season weren't
   bad trades. They were a stale order book, simulated fills that couldn't happen, and
   a ledger that counted exits twice. Each one made the results look better than they
   were, which is exactly why they were worth finding.
