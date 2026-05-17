import uuid
import time
from event_bus import bus


class PositionManager:
    MAINTENANCE_MARGIN_RATE = 0.01
    MAKER_FEE = 0.0002
    TAKER_FEE = 0.0005
    LIQ_FEE = 0.005
    FUNDING_INTERVAL = 28800  # 8 hours in seconds; simulation compresses to 3600

    def __init__(self):
        self.positions: dict[str, dict] = {}
        self.balance = 1_000_000.0
        self.mark_price_calc = None

    @property
    def mark_price(self) -> float:
        return self.mark_price_calc.compute() if self.mark_price_calc else 0.0

    def liquidation_price(self, pos: dict) -> float:
        if pos['side'] == 'long':
            return pos['entry_price'] * (1 - 1 / pos['leverage'] + self.MAINTENANCE_MARGIN_RATE)
        else:
            return pos['entry_price'] * (1 + 1 / pos['leverage'] - self.MAINTENANCE_MARGIN_RATE)

    def breakeven_price(self, pos: dict) -> float:
        fee_rate = self.TAKER_FEE * 2
        if pos['side'] == 'long':
            return pos['entry_price'] * (1 + fee_rate)
        else:
            return pos['entry_price'] * (1 - fee_rate)

    def unrealized_pnl(self, pos: dict) -> float:
        mark = self.mark_price
        if pos['side'] == 'long':
            return (mark - pos['entry_price']) * pos['size']
        else:
            return (pos['entry_price'] - mark) * pos['size']

    def adl_risk(self, pos: dict) -> int:
        pnl = self.unrealized_pnl(pos)
        margin = pos['margin']
        if pnl <= 0:
            return 1
        ratio = pnl / margin
        if ratio < 0.2: return 1
        if ratio < 0.5: return 2
        if ratio < 1.0: return 3
        if ratio < 2.0: return 4
        return 5

    async def open_position(self, side: str, size: float, leverage: int,
                             entry_price: float, order_type: str,
                             tp_price: float = 0, sl_price: float = 0) -> dict | None:
        fee_rate = self.TAKER_FEE if order_type == 'market' else self.MAKER_FEE
        notional = size * entry_price
        margin_required = notional / leverage
        fee = notional * fee_rate

        if margin_required + fee > self.balance:
            return None

        self.balance -= (margin_required + fee)

        pos = {
            'id': str(uuid.uuid4()),
            'user_id': 'user',
            'side': side,
            'size': size,
            'entry_price': entry_price,
            'leverage': leverage,
            'margin': margin_required,
            'unrealized_pnl': 0.0,
            'open_time': time.time(),
            'tp_price': tp_price,
            'sl_price': sl_price,
        }
        self.positions[pos['id']] = pos
        return pos

    def set_tpsl(self, pos_id: str, tp_price: float = 0, sl_price: float = 0) -> bool:
        """Set or update TP/SL prices on an existing position."""
        pos = self.positions.get(pos_id)
        if not pos:
            return False
        if tp_price > 0:
            pos['tp_price'] = tp_price
            # Validate sane TP
            if pos['side'] == 'long' and tp_price <= pos['entry_price']:
                return False
            if pos['side'] == 'short' and tp_price >= pos['entry_price']:
                return False
        if sl_price > 0:
            pos['sl_price'] = sl_price
            if pos['side'] == 'long' and sl_price >= pos['entry_price']:
                return False
            if pos['side'] == 'short' and sl_price <= pos['entry_price']:
                return False
        return True

    def clear_tpsl(self, pos_id: str) -> bool:
        """Remove TP/SL from a position."""
        pos = self.positions.get(pos_id)
        if not pos:
            return False
        pos['tp_price'] = 0
        pos['sl_price'] = 0
        return True

    def check_tpsl(self, mark_price: float) -> list[tuple[dict, str]]:
        """Return positions whose TP or SL has been hit. mark_price is passed in
        by the caller to avoid coupling to the engine/mark-price module."""
        triggered = []
        for pos in self.positions.values():
            tp = pos.get('tp_price', 0)
            sl = pos.get('sl_price', 0)
            if pos['side'] == 'long':
                if tp > 0 and mark_price >= tp:
                    triggered.append((pos, 'tp'))
                elif sl > 0 and mark_price <= sl:
                    triggered.append((pos, 'sl'))
            else:
                if tp > 0 and mark_price <= tp:
                    triggered.append((pos, 'tp'))
                elif sl > 0 and mark_price >= sl:
                    triggered.append((pos, 'sl'))
        return triggered

    async def close_position(self, pos_id: str, returned_margin: float):
        pos = self.positions.pop(pos_id, None)
        if pos:
            self.balance += returned_margin

    def add_fill(self, pos_id: str, fill_price: float, fill_size: float):
        """Update a position after an additional fill (partial execution)."""
        pos = self.positions.get(pos_id)
        if not pos:
            return
        # VWAP entry price
        old_notional = pos['entry_price'] * pos['size']
        new_notional = fill_price * fill_size
        pos['size'] += fill_size
        pos['entry_price'] = (old_notional + new_notional) / pos['size']
        # Additional margin for the new size
        additional_margin = fill_size * fill_price / pos['leverage']
        pos['margin'] += additional_margin
        self.balance -= additional_margin

    async def settle_funding(self, rate: float):
        mark = self.mark_price
        for pos in self.positions.values():
            notional = pos['size'] * mark
            funding_payment = notional * rate
            if pos['side'] == 'long':
                pos['margin'] -= funding_payment
            else:
                pos['margin'] += funding_payment

            if pos['margin'] <= 0:
                await bus.publish('liquidation.trigger', pos)

    def get_all(self) -> list[dict]:
        mark = self.mark_price
        for pos in self.positions.values():
            pos['unrealized_pnl'] = self.unrealized_pnl(pos)
        return list(self.positions.values())
