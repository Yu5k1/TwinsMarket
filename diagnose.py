"""Quick diagnostic: print raw WS data."""
import json, time, websocket, threading

WS_URL = "ws://127.0.0.1:8889/ws"
result = [None]

def on_message(ws, msg):
    if result[0] is None:
        result[0] = json.loads(msg)
        ws.close()

def on_open(ws):
    pass

ws = websocket.WebSocketApp(WS_URL, on_message=on_message, on_open=on_open)
t = threading.Thread(target=ws.run_forever, daemon=True)
t.start()

for _ in range(50):
    if result[0] is not None:
        break
    time.sleep(0.1)

data = result[0]
if not data:
    print("No data received")
else:
    print("=== Price fields ===")
    for k in ['price', 'mid_price', 'mark_price', 'index_price', 'spread', 'market_state']:
        print(f"  {k}: {data.get(k)}")

    print("\n=== 1m kline bars (last 5) ===")
    m1 = (data.get('klines', {})).get('1m', {})
    bars = m1.get('bars', [])
    print(f"  Total bars: {len(bars)}")
    for bar in bars[-5:]:
        print(f"  t={bar.get('t')} o={bar.get('o')} h={bar.get('h')} l={bar.get('l')} c={bar.get('c')} v={bar.get('v')}")

    print("\n=== Orderbook top 5 asks/bids ===")
    ob = data.get('orderbook', {})
    for side in ['asks', 'bids']:
        print(f"  {side}:")
        for level in (ob.get(side) or [])[:5]:
            print(f"    {level[0]}: {level[1]}")

    print(f"\n=== Trades (last 5) ===")
    for t in (data.get('trades') or [])[-5:]:
        print(f"  {t.get('side')} {t.get('size')} @ {t.get('price')} (aggressor={t.get('aggressor')})")
