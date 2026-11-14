# Kalshi API notes (STEP 0 — read from live docs, not training data)

Fetched from `docs.kalshi.com` and `kalshi.com/fee-schedule` on **2026-06-23**.
Endpoints and field names drift; everything host/path-shaped is config-driven so you can
re-point it without code changes. **Re-verify before a real run.**

## Authentication (RSA API key)

Three headers on every authenticated request:

| Header | Value |
| --- | --- |
| `KALSHI-ACCESS-KEY` | the API key **id** (not the secret) |
| `KALSHI-ACCESS-TIMESTAMP` | current time in **milliseconds**, as a string |
| `KALSHI-ACCESS-SIGNATURE` | base64 RSA-PSS signature |

Signed message = `timestamp + METHOD + path` where `path` includes the `/trade-api/v2`
prefix and **excludes the query string**.

Signing algorithm (verbatim from docs):
- padding: **PSS**, MGF1(SHA-256)
- salt length: **digest length** (`PSS.DIGEST_LENGTH`, 32 bytes)
- hash: **SHA-256**
- output: **base64**

```python
signature = private_key.sign(
    message,
    padding.PSS(mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH),
    hashes.SHA256(),
)
```

Public market-data endpoints (`/markets`, orderbook, etc.) accept but do **not** require
these headers; everything under `/portfolio` requires them.

## Hosts (config-driven — docs show several historically)

Docs as of this fetch returned `https://external-api.kalshi.com` (prod) /
`https://external-api.demo.kalshi.co` (demo). The long-standing production host is
`https://api.elections.kalshi.com`. Because this is genuinely ambiguous and the whole point
of STEP 0 is "don't guess", both REST and WS base URLs live in `config.yaml`. Defaults below
match what the docs returned today; **confirm the host your key is provisioned for.**

- REST base: `https://api.elections.kalshi.com` + path prefix `/trade-api/v2`
- WS base:   `wss://api.elections.kalshi.com/trade-api/ws/v2`

## REST (read-only paths used here)

- `GET /trade-api/v2/markets` — list/filter markets
- `GET /trade-api/v2/markets/{ticker}` — single market (status, result, prices)
- `GET /trade-api/v2/markets/{ticker}/orderbook?depth=N` — resting bids

Orderbook response: an `orderbook` (or newer `orderbook_fp`) object with `yes` and `no`
arrays. Each element is `[price, quantity]`. **Both arrays are resting BIDS only**, sorted
ascending; the best (highest) bid is the **last** element. Newer API encodes these as
`yes_dollars` / `no_dollars` with string dollar prices (`"0.42"`) and fixed-point sizes
(`"13.00"`). The client normalizes either encoding to integer cents + integer contracts.

YES/NO duality (prices in cents, YES+NO = 100):
- best YES bid  = max price in `yes`
- best YES ask  = 100 − (max price in `no`)   ← to BUY yes you lift NO bids
- best NO  bid  = max price in `no`
- best NO  ask  = 100 − (max price in `yes`)

Market status lifecycle: `initialized → open → closed → determined → settled/finalized`.
On settlement, `result` ∈ {`yes`, `no`, ...} and a settlement timestamp is populated.

## WebSocket

- Connect to the WS base URL with the same signed `KALSHI-ACCESS-*` headers on the HTTP
  handshake. WS signature message = `timestamp + "GET" + "/trade-api/ws/v2"`.
- Subscribe:
  ```json
  {"id": 1, "cmd": "subscribe",
   "params": {"channels": ["orderbook_delta", "ticker"],
              "market_tickers": ["MKT1", "MKT2"]}}
  ```
- Server sends `orderbook_snapshot` (full book) then incremental `orderbook_delta`.
  **VERIFIED LIVE 2026-06-23** against `KXWCGAME-26JUN23PORUZB-*` — the live format
  differs from the older docs examples, so the parser tolerates all three:
  ```json
  // snapshot (LIVE): price in dollars, size fixed-point (fractional contracts)
  {"type":"orderbook_snapshot","sid":1,"seq":1,
   "msg":{"market_ticker":"...","yes_dollars_fp":[["0.0100","1707575.34"], ...],
                                 "no_dollars_fp":[ ... ]}}
  // delta (LIVE):
  {"type":"orderbook_delta","sid":1,"seq":5,
   "msg":{"market_ticker":"...","price_dollars":"0.9600","delta_fp":"-29124.00","side":"yes",
          "ts_ms":1782235416956}}
  ```
  Accepted level keys: `yes`/`no` (int cents), `yes_dollars`/`no_dollars`,
  `yes_dollars_fp`/`no_dollars_fp`. Accepted delta keys: `price`+`delta` or
  `price_dollars`+`delta_fp`. All normalized to integer cents + integer contracts.
- `ticker` channel (LIVE): `{market_ticker, price_dollars, yes_bid_dollars,
  yes_ask_dollars, volume_fp, ...}`.
- **`seq` is a CONNECTION-level counter** (monotonic across ALL messages on the socket),
  NOT per-market. Track one expected seq for the whole connection; a break means dropped
  messages → resync (debounced). A per-market seq check causes a false-gap resubscribe storm.

## Fee schedule (kalshi.com/fee-schedule, "Feb 2026")

Parabolic, symmetric around 50¢, rounded **up** to the next cent. `C` = contracts,
`P` = price in **dollars** (0–1):

- taker / general: `fees = ceil(0.07 · C · P · (1−P))` dollars
- maker:           `fees = ceil(0.0175 · C · P · (1−P))` dollars  (25% of taker)

Max fee is at P=0.50 → 1.75¢/contract (taker). Some series carry special rates; the fee is
implemented as a **pluggable function** (`engine/fees.py`) with the rate in `config.yaml`,
so a per-series override is a one-line change. Re-confirm the World Cup series rate.

## Sources
- https://docs.kalshi.com/getting_started/api_keys
- https://docs.kalshi.com/getting_started/quick_start_authenticated_requests
- https://docs.kalshi.com/getting_started/quick_start_websockets
- https://docs.kalshi.com/getting_started/orderbook_responses
- https://docs.kalshi.com/api-reference/market/get-market
- https://kalshi.com/fee-schedule  (PDF: https://kalshi.com/docs/kalshi-fee-schedule.pdf)
