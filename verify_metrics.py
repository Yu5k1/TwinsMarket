"""Connect to WebSocket, collect 60s of data, verify 5 spec metrics."""
import json
import time
import sys
import numpy as np
import websocket

WS_URL = "ws://127.0.0.1:8889/ws"

samples = []
start_time = None


def on_message(ws, message):
    global start_time
    data = json.loads(message)
    samples.append((time.time(), data))


def on_error(ws, error):
    print(f"WS error: {error}")


def on_close(ws, status, msg):
    print("WS closed")


def on_open(ws):
    global start_time
    start_time = time.time()
    print(f"Connected, collecting 60s...")


ws = websocket.WebSocketApp(WS_URL,
                            on_message=on_message,
                            on_error=on_error,
                            on_close=on_close,
                            on_open=on_open)

import threading
t = threading.Thread(target=ws.run_forever)
t.daemon = True
t.start()

while start_time is None:
    time.sleep(0.1)

time.sleep(60)
ws.close()

print(f"\n{'='*60}")
print(f"Collected {len(samples)} snapshots (~{len(samples)//60}/s)")
print(f"{'='*60}")

if not samples:
    print("FAIL: No samples received!")
    sys.exit(1)

# ─── Parse first and last snapshots ───
first = samples[0][1]
last = samples[-1][1]

# Price fields are at top level
mid_first = float(first.get('mid_price', 0))
mid_last = float(last.get('mid_price', 0))
mark_last = float(last.get('mark_price', 0))
index_last = float(last.get('index_price', 0))
spread_last = float(last.get('spread', 0))
market_state = last.get('market_state', 'unknown')

print(f"\nMid price:  {mid_first:.2f} → {mid_last:.2f}")
print(f"Mark price: {mark_last:.2f}")
print(f"Index:      {index_last:.2f}")
print(f"Spread:     {spread_last:.2f}")
print(f"State:      {market_state}")
if mid_last > 0 and index_last > 0:
    dev = abs(mid_last - index_last) / index_last * 100
    print(f"Mid/Idx dev: {dev:.3f}%")

# ─── K-line data ───
klines = last.get('klines', {})
m1 = klines.get('1m', {})
m1_bars = m1.get('bars', [])

print(f"\n1m bars available: {len(m1_bars)}")

# ─── Metric 1: 1-min candle amplitude (0.05% - 0.5%) ───
print(f"\n[1] 1-min candle amplitude (last 10):")
amps = []
for bar in m1_bars[-10:]:
    h, l, o = float(bar['h']), float(bar['l']), float(bar['o'])
    if o > 0 and h >= l:
        amps.append((h - l) / o * 100)

if amps:
    for i, a in enumerate(amps):
        print(f"    bar -{len(amps)-i}: {a:.3f}%")
    avg_amp = sum(amps) / len(amps)
    print(f"    Average: {avg_amp:.3f}%  (target: 0.05% - 0.5%)")
    print(f"    {'PASS' if 0.05 <= avg_amp <= 0.5 else 'WARN'}")
else:
    print("    No valid bars yet")

# ─── Metric 2: 10-bar net displacement (<1%) ───
print(f"\n[2] 10-bar net displacement:")
if len(m1_bars) >= 10:
    bars10 = m1_bars[-10:]
    first_c = float(bars10[0]['c'])
    last_c = float(bars10[-1]['c'])
    if first_c > 0:
        disp = abs(last_c - first_c) / first_c * 100
        print(f"    {first_c:.2f} → {last_c:.2f} = {disp:.4f}%  (target: <1%)")
        print(f"    {'PASS' if disp < 1.0 else 'WARN'}")
else:
    print(f"    Not enough bars ({len(m1_bars)})")

# ─── Metric 3: sigma_t steady state (0.0002 - 0.0005) ───
print(f"\n[3] Volatility (sigma_t equivalent):")
# Not in push payload, compute from 1m bar log returns
if len(m1_bars) >= 20:
    closes = [float(b['c']) for b in m1_bars[-20:]]
    log_rets = []
    for i in range(1, len(closes)):
        if closes[i-1] > 0 and closes[i] > 0:
            log_rets.append(np.log(closes[i] / closes[i-1]))
    if log_rets:
        emp_sigma = float(np.std(log_rets))
        # 1-minute sigma → per-tick (50ms tick) equivalent for comparison
        # sigma_1m = sigma_tick * sqrt(1200) since 1200 ticks per minute (50ms each)
        # sigma_tick = sigma_1m / sqrt(1200)
        emp_tick = emp_sigma / np.sqrt(1200)
        emp_annual = emp_sigma * np.sqrt(1440 * 365)
        print(f"    σ_1m (empirical, 20 obs):     {emp_sigma:.6f}")
        print(f"    σ_tick equivalent (÷√1200):   {emp_tick:.6f}")
        print(f"    Target σ_t range:             0.0002 - 0.0005")
        print(f"    Annualized:                   {emp_annual*100:.2f}%")
        if 0.0002 <= emp_tick <= 0.0005:
            print(f"    PASS")
        elif emp_tick < 0.0002:
            print(f"    WARN: below target (conservative)")
        else:
            print(f"    WARN: above target")

# ─── Metric 4: 5-min price/start ratio (0.97 - 1.03) ───
print(f"\n[4] 5-min price/start ratio:")
if mid_first > 0:
    ratio = mid_last / mid_first
    print(f"    {mid_first:.2f} → {mid_last:.2f} = {ratio:.4f}  (target: 0.97 - 1.03)")
    print(f"    {'PASS' if 0.97 <= ratio <= 1.03 else 'WARN'}")
else:
    print(f"    No valid mid price")

# ─── Metric 5: panic duration (<5s) ───
print(f"\n[5] Panic state duration:")
market_states = [(ts, s.get('market_state', '?')) for ts, s in samples]

panic_periods = []
in_panic = False
panic_start = None
for ts, ms in market_states:
    if ms == 'panic' and not in_panic:
        in_panic = True
        panic_start = ts
    elif ms != 'panic' and in_panic:
        in_panic = False
        panic_periods.append(ts - panic_start)
if in_panic:
    panic_periods.append(time.time() - panic_start)

if panic_periods:
    for i, d in enumerate(panic_periods):
        print(f"    #{i+1}: {d:.1f}s  {'PASS' if d < 5 else 'WARN >5s'}")
else:
    print(f"    No panic periods — PASS")

# ─── Additional: bar gap check ───
print(f"\n[Bonus] Adjacent bar open/prev_close gap:")
if len(m1_bars) >= 20:
    gaps = []
    bars_for_gap = m1_bars[-20:]
    for i in range(1, len(bars_for_gap)):
        prev_c = float(bars_for_gap[i-1]['c'])
        cur_o = float(bars_for_gap[i]['o'])
        if prev_c > 0:
            gaps.append(abs(cur_o - prev_c) / prev_c * 100)
    if gaps:
        avg_gap = sum(gaps) / len(gaps)
        max_gap = max(gaps)
        print(f"    Average: {avg_gap:.4f}%  Max: {max_gap:.4f}%  (target <0.3%)")
        print(f"    {'PASS' if avg_gap < 0.3 else 'WARN'}")

print(f"\n{'='*60}")
print("Verification complete.")
