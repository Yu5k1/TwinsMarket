"""Repair data/state.json after a price runaway.

Scans each timeframe's K-line history for the last "sane" bar (all OHLC
within 100..1_000_000 and high/low ratio < 1.10), drops any subsequent
corrupted bars, clears the open `current` bar, and re-anchors oracle
index_price/mu/theta to that last clean close.
"""
import json
import os
import shutil
import time

STATE_PATH = './data/state.json'
DEFAULT_PRICE = 23500.0


def is_sane(bar):
    if not bar:
        return False
    try:
        o, h, l, c = float(bar['o']), float(bar['h']), float(bar['l']), float(bar['c'])
    except (KeyError, TypeError, ValueError):
        return False
    if not all(100 < x < 1_000_000 for x in (o, h, l, c)):
        return False
    if l <= 0 or h / l > 1.10:
        return False
    return True


def main():
    if not os.path.exists(STATE_PATH):
        print(f"[fix_state] {STATE_PATH} not found — nothing to repair")
        return

    with open(STATE_PATH) as f:
        state = json.load(f)

    backup = STATE_PATH + '.bak'
    shutil.copy(STATE_PATH, backup)
    print(f"[fix_state] Backed up to {backup}")

    klines = state.get('klines', {})
    anchor_close = None

    for tf, data in klines.items():
        history = data.get('history', []) or []
        # Walk backward from the end, drop bars until we hit a sane one.
        last_sane_idx = -1
        for i in range(len(history) - 1, -1, -1):
            if is_sane(history[i]):
                last_sane_idx = i
                break
        dropped = len(history) - 1 - last_sane_idx
        if last_sane_idx >= 0:
            data['history'] = history[:last_sane_idx + 1]
            tf_anchor = float(history[last_sane_idx]['c'])
        else:
            data['history'] = []
            tf_anchor = None

        # Discard `current` — it almost always carries the freshest corruption,
        # and even if clean it'd be misaligned with the trimmed history.
        data['current'] = None

        print(f"  {tf}: trimmed {dropped} bad bar(s), {len(data['history'])} kept, "
              f"anchor={tf_anchor}")
        # Use 1m close as primary anchor.
        if tf == '1m' and tf_anchor:
            anchor_close = tf_anchor

    if anchor_close is None:
        # Fallback: any timeframe's anchor, else default.
        for tf, data in klines.items():
            hist = data.get('history') or []
            if hist:
                anchor_close = float(hist[-1]['c'])
                break
    if anchor_close is None:
        anchor_close = DEFAULT_PRICE
        print(f"[fix_state] No sane bars in any timeframe — falling back to {DEFAULT_PRICE}")

    state['oracle'] = {
        'index_price':   anchor_close,
        'mu':            anchor_close,
        'theta':         anchor_close,
        'sigma_t':       0.00025,
        'sigma2':        0.00025 ** 2,
        'trend_bias':    0.0,
        'recent_return': 0.0,
        'market_state':  'calm',
    }
    state['timestamp'] = time.time()

    with open(STATE_PATH, 'w') as f:
        json.dump(state, f)
    print(f"[fix_state] Oracle anchored at {anchor_close:.2f}; state.json rewritten.")


if __name__ == '__main__':
    main()
