import asyncio
import json
import time
import numpy as np
import uvicorn
import webbrowser
import os
import sys

# Ensure project root is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from event_bus import bus
from clock import SimClock
from oracle import Oracle
from engine import MatchingEngine, Order
from presets import MM_CONFIGS, PRESETS
from contracts.positions import PositionManager
from contracts.mark_price import MarkPriceCalculator
from contracts.liquidation import LiquidationMonitor
from contracts.funding import FundingRateCalculator
from agents.market_maker import MarketMaker
from agents.noise_trader import NoiseTrader
from agents.arbitrageur import Arbitrageur
from agents.trend_follower import TrendFollower
from news import NewsSystem
from faucet import Faucet
from journal import TradeJournal
from persistence import periodic_save, save_state
from ws_push import WSPushManager
from api import app

from fastapi import WebSocket, WebSocketDisconnect


# ─── Global module instances ───
engine: MatchingEngine | None = None
oracle: Oracle | None = None
position_manager: PositionManager | None = None
mark_calc: MarkPriceCalculator | None = None
liquidation_monitor: LiquidationMonitor | None = None
funding_calc: FundingRateCalculator | None = None
mm_aggressive: MarketMaker | None = None
mm_conservative: MarketMaker | None = None
noise_trader: NoiseTrader | None = None
arbitrageur: Arbitrageur | None = None
trend_follower: TrendFollower | None = None
news_system: NewsSystem | None = None
faucet: Faucet | None = None
trade_journal: TradeJournal | None = None
ws_push: WSPushManager | None = None


# ─── WebSocket endpoint ───
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws_push.connect(ws)
    try:
        while True:
            await ws.receive_text()  # Keep alive, ignore client messages
    except WebSocketDisconnect:
        ws_push.disconnect(ws)
    except Exception:
        ws_push.disconnect(ws)


async def cold_start():
    """Warm up: generate 30 days of OHLCV history. Skips if saved klines are recent."""
    # Check if saved klines are recent enough to skip regeneration
    SAVE_FILE = './data/state.json'
    if os.path.exists(SAVE_FILE):
        try:
            with open(SAVE_FILE) as f:
                saved = json.load(f)
            klines_data = saved.get('klines', {})
            # If we have 1m bars within the last hour, skip cold start
            m1 = klines_data.get('1m', {})
            current_ts = m1.get('current', {}).get('t', 0)
            if current_ts and (time.time() - current_ts) < 3600:
                # Restore last price from klines — persistence doesn't save mid_price,
                # so without this market makers see mid==0 and never post quotes.
                cur_close = (m1.get('current') or {}).get('c')
                hist_close = m1['history'][-1]['c'] if m1.get('history') else None
                # Reject corrupted closes (negative or absurdly low/high) — scan history
                # backward for the last sane bar so we never seed the engine with a bad price.
                def _sane(p):
                    return p is not None and 100 < float(p) < 1_000_000
                last_close = cur_close if _sane(cur_close) else hist_close
                if not _sane(last_close):
                    for b in reversed(m1.get('history', [])):
                        if _sane(b.get('c')):
                            last_close = b['c']
                            break
                if _sane(last_close):
                    engine.last_price = float(last_close)
                    engine.mid_price  = float(last_close)
                    engine.anchor_price = float(last_close)
                    # Restore oracle anchors from saved oracle state, falling back to K-line close.
                    # Critical: if saved index drifts >5% from last_close we *must* re-anchor,
                    # otherwise arbitrageur fires a giant market order on first tick.
                    if 'oracle' in saved:
                        s = saved['oracle']
                        saved_index = float(s.get('index_price', last_close))
                        diverged = (saved_index <= 0
                                    or abs(saved_index - float(last_close)) / float(last_close) > 0.05)
                        if diverged:
                            oracle.index_price = float(last_close)
                            oracle.mu          = float(last_close)
                            oracle.theta       = float(last_close)
                            oracle.trend_bias   = 0.0
                            oracle.recent_return = 0.0
                        else:
                            oracle.index_price = saved_index
                            oracle.mu          = float(s.get('mu',    saved_index))
                            oracle.theta       = float(s.get('theta', saved_index))
                            oracle.trend_bias   = float(s.get('trend_bias',   0.0))
                            oracle.recent_return = float(s.get('recent_return', 0.0))
                        oracle.sigma_t      = float(s.get('sigma_t',      oracle.sigma_t))
                        oracle.sigma2       = float(s.get('sigma2',       oracle.sigma2))
                        oracle.market_state = s.get('market_state', 'calm')
                    else:
                        oracle.index_price = float(last_close)
                        oracle.mu          = float(last_close)
                        oracle.theta       = float(last_close)

                # Seed OFI window so spread/inventory calculations work immediately
                for _ in range(50):
                    side = 'buy' if np.random.random() < 0.5 else 'sell'
                    engine.ofi_window.append((float(np.random.exponential(8.0)), side))

                print("[Startup] K-line data is fresh, skipping regeneration")
                print(f"  1m: {len(m1.get('history',[]))+1} bars")
                for tf in ['5m','15m','1h','4h','1D']:
                    tf_data = klines_data.get(tf, {})
                    print(f"  {tf}: {len(tf_data.get('history',[]))+1} bars")
                print(f"[Startup] Restored price: {engine.mid_price:.2f}")

                # Smooth exponential ramp on both MMs from a single timestamp.
                _now = time.monotonic()
                mm_aggressive._startup_time  = _now
                mm_conservative._startup_time = _now
                # Stagger noise/trend agents so the first few seconds don't
                # see them sweeping the still-coalescing book.
                noise_trader.startup_delay_ticks   = 30
                trend_follower.startup_delay_ticks = 30
                await mm_aggressive.tick()
                await mm_conservative.tick()
                return
        except Exception:
            pass

    print("[Startup] Generating 30-day market history...")

    init_price = 23500.0
    total_days = 30

    # ═══ Generate SUB-MINUTE ticks (every 10 seconds) for realistic OHLC ═══
    TICKS_PER_MINUTE = 6  # one tick every 10 seconds
    total_minutes = total_days * 24 * 60
    total_ticks = total_minutes * TICKS_PER_MINUTE  # ~259K ticks

    # Per-tick sigma aligned to oracle steady-state sigma_t ≈ 0.0003 per 100ms.
    # Per 10s tick: 0.0003 × √(10s / 0.1s) = 0.0003 × 10 = 0.003
    oracle_sigma_t = 0.0003
    tick_vol = oracle_sigma_t * np.sqrt(10.0 / 0.1)  # ≈ 0.003

    np.random.seed(42)

    # Build per-tick vol with clustering matching oracle state multipliers.
    # calm=0.5×, trend=1.0×, volatile=2.0×, panic=4.0×
    vol_series = np.full(total_ticks, tick_vol)
    n_bursts = np.random.randint(30, 50)  # ~1-2 volatile periods per day
    for _ in range(n_bursts):
        start = np.random.randint(0, total_ticks - 180)
        duration = np.random.randint(60, 720)   # 10 min to 2 hours at 10s/tick
        mult = np.random.choice([0.5, 1.0, 2.0, 4.0], p=[0.55, 0.15, 0.20, 0.10])
        vol_series[start:start + duration] *= mult

    log_returns = np.random.randn(total_ticks) * vol_series
    log_returns += 0.0001 / (1440 * TICKS_PER_MINUTE)  # tiny drift

    # Jump shocks aligned to oracle: normal (94%) at 0.2-0.8%, large (6%) at 1-4%
    n_jumps = np.random.randint(40, 70)
    jump_idx = np.random.choice(total_ticks, n_jumps, replace=False)
    is_large = np.random.random(n_jumps) < 0.06
    jump_mag = np.where(
        is_large,
        np.random.uniform(0.01, 0.04, n_jumps),
        np.random.uniform(0.002, 0.008, n_jumps)
    )
    jump_sign = np.where(np.random.random(n_jumps) < 0.5, -1, 1)
    log_returns[jump_idx] += jump_sign * jump_mag
    log_prices = np.log(init_price) + np.cumsum(log_returns)
    prices_tick = np.exp(log_prices)

    # ═══ Aggregate ticks → 1-minute OHLCV bars ═══
    # Per-bar volume generated directly from lognormal calibrated to live
    # measurement: 5-min real-time avg = 65.4 per 1m bar.
    TARGET_VOL_1M = 65.4
    mu_vol = np.log(TARGET_VOL_1M)
    vol_1m = np.random.lognormal(mean=mu_vol, sigma=0.5, size=total_minutes)

    now = time.time()
    bars_1m = []
    for m in range(total_minutes):
        start = m * TICKS_PER_MINUTE
        end = start + TICKS_PER_MINUTE
        chunk_p = prices_tick[start:end]
        bar_ts = now - (total_minutes - m) * 60
        bar_start = int(bar_ts / 60) * 60
        bars_1m.append({
            't': bar_start, 'tf': '1m',
            'o': float(chunk_p[0]),
            'h': float(np.max(chunk_p)),
            'l': float(np.min(chunk_p)),
            'c': float(chunk_p[-1]),
            'v': float(vol_1m[m]),
        })

    # ═══ Aggregate 1m bars → higher timeframes ═══
    def build_from_1m(tf_minutes: int, tf_name: str) -> list[dict]:
        step = tf_minutes
        bars = []
        for start_idx in range(0, len(bars_1m) - step + 1, step):
            chunk = bars_1m[start_idx:start_idx + step]
            bar_ts = chunk[0]['t']
            bar_start = int(bar_ts / (tf_minutes * 60)) * (tf_minutes * 60)
            bars.append({
                't': bar_start, 'tf': tf_name,
                'o': chunk[0]['o'],
                'h': max(b['h'] for b in chunk),
                'l': min(b['l'] for b in chunk),
                'c': chunk[-1]['c'],
                'v': sum(b['v'] for b in chunk), 
            })
        return bars

    tf_map = {'1m': 1, '5m': 5, '15m': 15, '1h': 60, '4h': 240, '1D': 1440}
    first_o = None
    last_c = None
    for tf_name, tf_min in tf_map.items():
        if tf_name == '1m':
            bars = bars_1m
        else:
            bars = build_from_1m(tf_min, tf_name)

        # Align bar opens to previous close so adjacent bars are continuous.
        for i in range(1, len(bars)):
            bars[i]['o'] = bars[i-1]['c']

        builder = engine.kline_builders[tf_name]
        if len(bars) > 1:
            builder.history = bars[:-1]
            builder.current = bars[-1]
            if len(builder.history) > 800:
                builder.history = builder.history[-800:]
        else:
            builder.history = bars
            builder.current = None

        if first_o is None: first_o = bars[0]['o']
        last_c = bars[-1]['c']
        print(f"  {tf_name}: {len(builder.get_bars(999))} bars "
              f"(range {bars[0]['c']:.1f} → {bars[-1]['c']:.1f})")

    engine.last_price = last_c or init_price
    engine.mid_price = last_c or init_price
    engine.anchor_price = last_c or init_price

    # Align oracle anchors to the regenerated K-line endpoint. Without this,
    # oracle.index_price stays at init_price (23500) while engine.mid_price
    # tracks the random-walk endpoint (could be anywhere from 14k–38k), and
    # arbitrageur sees a 30%+ deviation on first tick → massive market order
    # → runaway K-line.
    seed_price = float(last_c or init_price)
    oracle.index_price = seed_price
    oracle.mu          = seed_price
    oracle.theta       = seed_price
    oracle.trend_bias   = 0.0
    oracle.recent_return = 0.0

    # Seed OFI window
    for _ in range(min(200, total_ticks)):
        side = 'buy' if np.random.random() < 0.5 else 'sell'
        engine.ofi_window.append((float(np.random.exponential(TARGET_VOL_1M / TICKS_PER_MINUTE)), side))

    # Save immediately so restart doesn't need regeneration
    await save_state(position_manager, liquidation_monitor, engine, trade_journal, oracle)
    print("[Startup] History saved to disk")

    # Smooth exponential startup ramp — half-life 7 s, effectively done at 20 s.
    _now = time.monotonic()
    mm_aggressive._startup_time  = _now
    mm_conservative._startup_time = _now
    # Suppress noise/trend agents for the first 1.5 s so the first K-line bar
    # isn't dominated by them sweeping a still-thin book.
    noise_trader.startup_delay_ticks   = 30
    trend_follower.startup_delay_ticks = 30

    # Do one round of quoting so the orderbook isn't empty
    await mm_aggressive.tick()
    await mm_conservative.tick()

    print("[Startup] Warmup complete, running normally")


async def push_to_frontend():
    """Registered as a clock tick handler to push data to WebSocket clients."""
    await ws_push.push_tick()


async def news_tick_wrapper():
    """Wrapper so news system receives current market state."""
    if oracle and news_system:
        pct_change = oracle.recent_return
        await news_system.tick(oracle.market_state, pct_change)


async def save_wrapper():
    """Periodic persistence."""
    await periodic_save(position_manager, liquidation_monitor, engine, trade_journal, oracle)


async def tpsl_check():
    """Check TP/SL triggers and close positions that have been hit."""
    mark = mark_calc.compute()
    triggered = position_manager.check_tpsl(mark)
    for pos, trigger_type in triggered:
        # Submit a market close order
        close_side = 'sell' if pos['side'] == 'long' else 'buy'
        order = Order(
            order_id=f"tpsl_{pos['id']}", owner='user',
            side=close_side, type='market',
            price=0.0, size=pos['size'],
        )
        trades = await engine.submit_order(order)
        close_price = trades[-1].price if trades else engine.mid_price
        pnl = position_manager.unrealized_pnl(pos)

        position_manager.clear_tpsl(pos['id'])
        await position_manager.close_position(pos['id'], pos['margin'] + pnl)

        if trade_journal:
            trade_journal.record(pos, close_price, pnl, trigger_type)


async def main():
    global engine, oracle, position_manager, mark_calc
    global liquidation_monitor, funding_calc
    global mm_aggressive, mm_conservative, noise_trader
    global arbitrageur, trend_follower, news_system, faucet, trade_journal, ws_push

    print("═" * 50)
    print("  TwinsMarket — AEN/USDT Perpetual Simulation")
    print("═" * 50)

    # ─── Init all modules ───
    engine = MatchingEngine()
    oracle = Oracle(init_price=23500.0)

    # Apply preset (mainstream = default)
    preset = PRESETS.get(os.environ.get('TWINS_PRESET', 'mainstream'), PRESETS['mainstream'])
    print(f"[Startup] Market preset: {preset['name']} — {preset['description']}")

    oracle.apply_volatility_mult(preset.get('oracle_volatility_mult', 1.0))
    if 'oracle_initial_state' in preset:
        oracle.set_initial_state(preset['oracle_initial_state'])

    position_manager = PositionManager()
    mark_calc = MarkPriceCalculator(engine)
    position_manager.mark_price_calc = mark_calc

    liquidation_monitor = LiquidationMonitor(engine, position_manager)
    funding_calc = FundingRateCalculator(mark_calc, oracle=oracle)

    mm_aggressive = MarketMaker(
        'mm_aggressive',
        preset.get('mm_aggressive', MM_CONFIGS['mm_aggressive']),
        engine,
        oracle=oracle
    )
    mm_conservative = MarketMaker(
        'mm_conservative',
        preset.get('mm_conservative', MM_CONFIGS['mm_conservative']),
        engine,
        oracle=oracle
    )

    noise_trader = NoiseTrader(engine)
    noise_trader.base_rate_mult = preset.get('noise_base_rate', 1.0)

    arbitrageur = Arbitrageur(engine, oracle=oracle)
    if 'arbitrageur_entry_threshold' in preset:
        arbitrageur.entry_threshold = preset['arbitrageur_entry_threshold']

    trend_follower = TrendFollower(engine)
    news_system = NewsSystem()
    faucet = Faucet(position_manager)
    trade_journal = TradeJournal()

    ws_push = WSPushManager(
        engine, position_manager, funding_calc,
        liquidation_monitor, news_system, oracle, faucet,
        trade_journal
    )

    # Inject into API module
    import api
    api.engine = engine
    api.position_manager = position_manager
    api.funding_calc = funding_calc
    api.liquidation_monitor = liquidation_monitor
    api.faucet = faucet
    api.news_system = news_system
    api.trade_journal = trade_journal
    api.setup_trade_handler()
    ws_push.pending_orders_ref = api._pending_user_orders

    # Subscribe to user impact
    async def on_user_impact(data):
        oracle.apply_user_impact(data['net_qty'], data['direction'])
    bus.subscribe('user.impact', on_user_impact)

    # ─── Always fresh start ───
    print("[Startup] Fresh session")
    if os.path.exists('./data/state.json'):
        os.remove('./data/state.json')

    # ─── Cold start warmup ───
    await cold_start()

    # ─── Register clock agents ───
    clock = SimClock()
    clock.register(2, oracle.tick, 'Oracle')
    clock.register(2, liquidation_monitor.tick, 'Liquidation')
    clock.register(2, tpsl_check, 'TPSL_Check')
    clock.register(2, arbitrageur.tick, 'Arbitrageur')
    clock.register(2, mm_aggressive.tick, 'MM_Aggressive')
    clock.register(3, mm_conservative.tick, 'MM_Conservative')
    clock.register(3, trend_follower.tick, 'TrendFollower')
    clock.register(4, noise_trader.tick, 'NoiseTrader')
    clock.register(600, funding_calc.tick, 'FundingRate')
    clock.register(24, news_tick_wrapper, 'NewsSystem')
    clock.register(1, push_to_frontend, 'FrontendPush')
    clock.register(1200, save_wrapper, 'Persistence')

    # ─── Serve frontend at root (after WS route registered, so WS takes priority) ───
    from fastapi.responses import HTMLResponse, FileResponse
    import os as _os

    @app.get("/")
    async def serve_index():
        path = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "frontend", "index.html")
        return HTMLResponse(open(path, encoding='utf-8').read())

    # Mount static files under /static prefix to avoid capturing /ws
    from fastapi.staticfiles import StaticFiles
    app.mount("/static", StaticFiles(directory="frontend"), name="static")

    # ─── Start FastAPI server ───
    config = uvicorn.Config(
        app, host='127.0.0.1', port=8888,
        log_level='warning'
    )
    server = uvicorn.Server(config)

    # Run server as background task
    server_task = asyncio.create_task(server.serve())
    await asyncio.sleep(0.5)

    # ─── Open browser ───
    print("[Startup] Opening browser at http://localhost:8888")
    webbrowser.open('http://localhost:8888')

    print("[Startup] All systems running. Press Ctrl+C to stop.")
    print("─" * 50)

    # ─── Run main clock ───
    try:
        await clock.run()
    except asyncio.CancelledError:
        pass
    finally:
        print("\n[Shutdown] Saving state...")
        await save_state(position_manager, liquidation_monitor, engine, trade_journal, oracle)
        print("[Shutdown] Done.")


if __name__ == '__main__':
    asyncio.run(main())
