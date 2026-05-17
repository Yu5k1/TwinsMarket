# TwinsMarket

A simulated AEN/USDT perpetual futures exchange that runs entirely on your machine. The market is driven by a synthetic price oracle and several autonomous agents — no real money, no real data feeds.

<img width="1906" height="945" alt="screen" src="https://github.com/user-attachments/assets/83802f3f-2fdc-4582-8878-73a862fe1556" />
---

## What it does

- **Realistic order book** — two market makers (aggressive + conservative) maintain 400 levels on each side, with 10 USDT price increments
- **Synthetic price oracle** — GARCH volatility, momentum, random jumps, and a four-state market regime machine (calm / trend / volatile / panic)
- **Autonomous agents** — arbitrageur, noise trader, and trend follower create organic market activity
- **Full perpetual contract mechanics** — leverage up to 25×, funding rate, liquidation, insurance fund, ADL
- **Take-profit / Stop-loss** — set TP/SL at order entry or on an existing position
- **30-day history** — generated from scratch on first launch via GBM with volatility clustering and jump shocks
- **Live UI** — candlestick chart (6 timeframes), order book, recent trades, open orders, position tracker, and trade history

---

## Quick start

```bash
pip install -r requirements.txt
python main.py
```

The browser opens automatically at `http://localhost:8888`.

### Market presets

Set the `TWINS_PRESET` environment variable before launching:

| Value | Description |
|-------|-------------|
| `mainstream` | Default. Tight spreads, deep book — closest to BTC/ETH feel |
| `retail` | Wide spreads, thin liquidity — small-cap altcoin behaviour |
| `extreme` | High volatility, panic state on start — simulates a crash or pump |

```bash
TWINS_PRESET=extreme python main.py
```

---

## How the price works

The oracle generates a new index price every 100 ms using a log-return model:

```
log_return = momentum + jump + micro_noise
index_price = prev_price × exp(log_return)
```

Volatility follows GARCH(1,1). The market state machine controls jump frequency and momentum strength. Market makers anchor their quotes to `oracle.index_price` — not to the order book mid — so there is no self-referential price feedback loop.

Mark price is a 30-minute TWAP of mid price, clamped to ±0.5% of index. PnL at close is settled against the actual VWAP of the closing trades, not against mark price.

---

## Project structure

```
main.py              Entry point, initialises all modules, starts the clock
clock.py             50 ms main loop, schedules agents by tick count
engine.py            Matching engine (limit / market / stop orders)
oracle.py            Synthetic price oracle
api.py               REST API (open, close, TP/SL, faucet, state)
ws_push.py           WebSocket push manager (50 ms snapshots)
presets.py           Market preset configurations
persistence.py       Save / load state to data/state.json

agents/
  market_maker.py    Dual MM instances (aggressive + conservative)
  arbitrageur.py     Keeps mid price close to index price
  noise_trader.py    Random retail-like order flow
  trend_follower.py  Breakout strategy with stop-loss orders

contracts/
  positions.py       Position lifecycle, margin, PnL, TP/SL
  mark_price.py      30-minute TWAP mark price
  funding.py         Funding rate calculation and settlement
  liquidation.py     Liquidation monitor, insurance fund, ADL

frontend/
  index.html         Single-file UI (HTML + CSS + JS)
```

---

## Dependencies

```
fastapi
uvicorn
numpy
sortedcontainers
pydantic
```
