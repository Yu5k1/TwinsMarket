import uuid
import time


class TradeJournal:
    def __init__(self):
        self.entries: list[dict] = []

    def record(self, pos: dict, close_price: float, pnl: float, close_reason: str):
        entry = {
            'id': str(uuid.uuid4()),
            'open_time': pos.get('open_time', 0),
            'close_time': time.time(),
            'side': pos['side'],
            'leverage': pos['leverage'],
            'size': pos['size'],
            'entry_price': pos['entry_price'],
            'close_price': close_price,
            'pnl': pnl,
            'pnl_pct': pnl / pos['margin'] * 100 if pos['margin'] > 0 else 0,
            'close_reason': close_reason,
            'margin_used': pos['margin'],
        }
        self.entries.insert(0, entry)
        if len(self.entries) > 1000:
            self.entries.pop()
