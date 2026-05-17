import json
import os
import time


SAVE_PATH = './data/state.json'


async def save_state(position_manager, liquidation_monitor, engine, trade_journal, oracle=None):
    os.makedirs('./data', exist_ok=True)
    state = {
        'balance': position_manager.balance,
        'insurance_fund': liquidation_monitor.insurance_fund,
        'klines': {
            tf: {
                'history': [b for b in builder.history[-200:]],
                'current': builder.current,
            }
            for tf, builder in engine.kline_builders.items()
        },
        'journal': trade_journal.entries[-500:],
        'timestamp': time.time(),
    }
    if oracle is not None:
        state['oracle'] = {
            'index_price':   oracle.index_price,
            'mu':            oracle.mu,
            'theta':         oracle.theta,
            'sigma_t':       oracle.sigma_t,
            'sigma2':        oracle.sigma2,
            'trend_bias':    oracle.trend_bias,
            'recent_return': oracle.recent_return,
            'market_state':  oracle.market_state,
        }
    with open(SAVE_PATH, 'w') as f:
        json.dump(state, f)


async def load_state(position_manager, liquidation_monitor, engine, trade_journal, oracle=None):
    if not os.path.exists(SAVE_PATH):
        return False
    try:
        with open(SAVE_PATH) as f:
            state = json.load(f)

        position_manager.balance = state.get('balance', position_manager.balance)
        liquidation_monitor.insurance_fund = state.get(
            'insurance_fund', liquidation_monitor.insurance_fund)

        # Restore K-line history
        klines_data = state.get('klines', {})
        for tf, data in klines_data.items():
            if tf in engine.kline_builders:
                builder = engine.kline_builders[tf]
                builder.history = [dict(b) for b in data.get('history', [])]
                builder.current = dict(data['current']) if data.get('current') else None

        # Restore oracle state so first K-line matches historical price
        if oracle is not None and 'oracle' in state:
            o = state['oracle']
            oracle.sigma_t      = float(o.get('sigma_t',      oracle.sigma_t))
            oracle.sigma2       = float(o.get('sigma2',       oracle.sigma2))
            oracle.mu           = float(o.get('mu',           oracle.mu))
            oracle.trend_bias   = float(o.get('trend_bias',   0.0))
            oracle.recent_return = float(o.get('recent_return', 0.0))
            oracle.market_state = o.get('market_state', oracle.market_state)
            oracle.index_price  = float(o.get('index_price',  oracle.index_price))
            oracle.theta        = float(o.get('theta',        oracle.index_price))

        # Restore journal
        journal_data = state.get('journal', [])
        if journal_data:
            trade_journal.entries = journal_data

        return True
    except Exception as e:
        print(f"[Persistence] Load failed, using defaults: {e}")
        return False


# Periodic save task
_save_counter = 0


async def periodic_save(position_manager, liquidation_monitor, engine, trade_journal, oracle=None):
    global _save_counter
    _save_counter += 1
    if _save_counter >= 1200:  # Every 1200 ticks = 60 seconds
        _save_counter = 0
        await save_state(position_manager, liquidation_monitor, engine, trade_journal, oracle)
