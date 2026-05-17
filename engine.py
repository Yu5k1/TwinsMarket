import asyncio
import uuid
import time
from dataclasses import dataclass, field
from sortedcontainers import SortedDict
from event_bus import bus


@dataclass
class Order:
    order_id: str
    owner: str
    side: str          # 'buy' | 'sell'
    type: str          # 'limit' | 'market' | 'stop'
    price: float
    size: float
    filled: float = 0.0
    status: str = 'open'
    timestamp: float = field(default_factory=time.time)
    stop_price: float = 0.0


@dataclass
class Trade:
    trade_id: str
    price: float
    size: float
    side: str
    aggressor: str
    passive: str
    timestamp: float


class KlineBuilder:
    TF_SECONDS = {'1m': 60, '5m': 300, '15m': 900, '1h': 3600, '4h': 14400, '1D': 86400}

    def __init__(self, tf: str):
        self.tf = tf
        self.seconds = self.TF_SECONDS[tf]
        self.current: dict | None = None
        self.history: list[dict] = []

    def update(self, price: float, volume: float):
        if price <= 0:
            return
        now = time.time()
        bar_start = int(now / self.seconds) * self.seconds

        if self.current is None or self.current['t'] != bar_start:
            if self.current:
                self.history.append(dict(self.current))
                if len(self.history) > 1000:
                    self.history.pop(0)
            open_price = self.current['c'] if self.current else price
            self.current = {
                't': bar_start, 'tf': self.tf,
                'o': open_price, 'h': price, 'l': price, 'c': price, 'v': 0.0
            }

        self.current['h'] = max(self.current['h'], price)
        self.current['l'] = min(self.current['l'], price)
        self.current['c'] = price
        self.current['v'] += volume

    def update_historical(self, price: float, volume: float, ts: float):
        """Used during cold start to seed K-lines."""
        bar_start = int(ts / self.seconds) * self.seconds

        if self.current is None or self.current['t'] != bar_start:
            if self.current:
                self.history.append(dict(self.current))
            self.current = {
                't': bar_start, 'tf': self.tf,
                'o': price, 'h': price, 'l': price, 'c': price, 'v': 0.0
            }

        self.current['h'] = max(self.current['h'], price)
        self.current['l'] = min(self.current['l'], price)
        self.current['c'] = price
        self.current['v'] += volume

    def get_bars(self, n: int = 100) -> list[dict]:
        bars = list(self.history[-n:])
        if self.current:
            bars.append(dict(self.current))
        return bars


class MatchingEngine:
    def __init__(self):
        # Order books: SortedDict for price-ordered access
        # asks: low to high; bids: high to low (via negative-key trick)
        self.asks: SortedDict = SortedDict()
        self.bids: SortedDict = SortedDict(lambda x: -x)

        self.stop_orders: list[Order] = []
        self.trades: list[Trade] = []
        self.order_map: dict[str, Order] = {}

        self.kline_builders: dict[str, KlineBuilder] = {}
        for tf in ['1m', '5m', '15m', '1h', '4h', '1D']:
            self.kline_builders[tf] = KlineBuilder(tf)

        self.last_price = 0.0
        self.mid_price = 0.0
        self.anchor_price = 0.0   # set by cold_start; sanity floor for _update_mid
        self._last_snapshot: dict = {'asks': [], 'bids': [], 'mid': 0.0}
        # Wall-clock timestamp of the last _update_mid that actually moved a
        # quote. Mark-price code uses this to detect a frozen mid (single-sided
        # book) and substitute index_price into the TWAP window.
        self.last_mid_update_time = time.monotonic()
        self.ofi_window: list[tuple[float, str]] = []

        # Reentrancy guard: prevent nested _match_market from corrupting order book
        self._matching_depth = 0
        self._deferred_stops: list[tuple[float, list[Order]]] = []

    @property
    def ofi(self) -> float:
        if not self.ofi_window:
            return 0.5
        buy_vol = sum(s for s, side in self.ofi_window if side == 'buy')
        total = sum(s for s, _ in self.ofi_window)
        return buy_vol / total if total > 0 else 0.5

    @property
    def spread(self) -> float:
        best_ask = next(iter(self.asks), None)
        best_bid = next(iter(self.bids), None)
        if best_ask and best_bid:
            return best_ask - best_bid
        return 0.0

    async def submit_order(self, order: Order) -> list[Trade]:
        self.order_map[order.order_id] = order
        trades = []

        if order.type == 'market':
            trades = await self._match_market(order)
        elif order.type == 'limit':
            trades = await self._match_limit(order)
        elif order.type == 'stop':
            self.stop_orders.append(order)

        # Process any stops that were deferred during matching
        # (only from the outermost submit_order call)
        if self._matching_depth == 0:
            while self._deferred_stops:
                _, stops = self._deferred_stops.pop(0)
                for stop in stops:
                    stop.type = 'market'
                    await self.submit_order(stop)

        return trades

    async def _match_market(self, order: Order) -> list[Trade]:
        self._matching_depth += 1
        try:
            trades = []
            remaining = order.size
            book = self.asks if order.side == 'buy' else self.bids

            # Retry up to 3 ticks (~150ms) for user orders if the book is empty —
            # MMs may be mid-repost. Internal/agent orders fail fast so they
            # don't block the matching loop.
            if not book and order.owner == 'user':
                for _ in range(3):
                    await asyncio.sleep(0.05)
                    book = self.asks if order.side == 'buy' else self.bids
                    if book:
                        break

            if not book:
                order.status = 'cancelled'
                await bus.publish('order.rejected', {
                    'reason': 'no_liquidity',
                    'order_id': order.order_id,
                    'owner': order.owner,
                    'side': order.side,
                    'size': order.size,
                })
                return trades

            for price in list(book.keys()):
                if remaining <= 0:
                    break
                level = book[price]
                # Snapshot the level to avoid mutation issues during iteration
                for passive in list(level):
                    if remaining <= 0:
                        break
                    if passive.owner == order.owner:
                        continue
                    avail = passive.size - passive.filled
                    if avail <= 0:
                        continue
                    fill = min(remaining, avail)
                    trade = await self._execute_fill(order, passive, price, fill)
                    trades.append(trade)
                    remaining -= fill
                    if passive.filled >= passive.size:
                        passive.status = 'filled'
                        try:
                            level.remove(passive)
                        except ValueError:
                            pass  # Already removed by nested match
                if not level and price in book:
                    del book[price]

            if remaining > 0:
                order.status = 'partial' if order.filled > 0 else 'cancelled'
            else:
                order.status = 'filled'

            return trades
        finally:
            self._matching_depth -= 1

    async def _match_limit(self, order: Order) -> list[Trade]:
        if order.side == 'buy':
            best_ask = next(iter(self.asks), None) if self.asks else None
            if best_ask is not None and order.price >= best_ask:
                return await self._match_market(order)
        else:
            best_bid = next(iter(self.bids), None) if self.bids else None
            if best_bid is not None and order.price <= best_bid:
                return await self._match_market(order)

        self._insert_order(order)
        return []

    def _insert_order(self, order: Order):
        if order.price <= 0:
            print(f"[Engine] WARNING: rejecting negative/zero limit price "
                  f"{order.price} from {order.owner}")
            order.status = 'cancelled'
            return
        if order.price > 10_000_000:
            print(f"[Engine] WARNING: rejecting extreme limit price "
                  f"{order.price:.2f} from {order.owner} — max allowed 10M")
            order.status = 'cancelled'
            return
        book = self.bids if order.side == 'buy' else self.asks
        if order.price not in book:
            book[order.price] = []
        book[order.price].append(order)
        self._update_mid()

    def _remove_order(self, order: Order):
        """Safely remove an order from the book. No-op if already gone."""
        book = self.bids if order.side == 'buy' else self.asks
        if order.price not in book:
            return
        try:
            book[order.price].remove(order)
        except (ValueError, KeyError):
            pass
        if order.price in book and not book[order.price]:
            del book[order.price]

    def cancel_order(self, order_id: str) -> bool:
        """Cancel a resting limit order. Returns True if found and removed."""
        order = self.order_map.get(order_id)
        if not order or order.status in ('filled', 'cancelled'):
            return False
        self._remove_order(order)
        order.status = 'cancelled'
        return True

    async def _execute_fill(self, aggressor: Order, passive: Order,
                             price: float, size: float) -> Trade:
        aggressor.filled += size
        passive.filled += size
        self.last_price = price

        trade = Trade(
            trade_id=str(uuid.uuid4()),
            price=price,
            size=size,
            side=aggressor.side,
            aggressor=aggressor.owner,
            passive=passive.owner,
            timestamp=time.time(),
        )
        self.trades.append(trade)

        for builder in self.kline_builders.values():
            builder.update(price, size)

        self.ofi_window.append((size, aggressor.side))
        if len(self.ofi_window) > 200:
            self.ofi_window.pop(0)

        self._update_mid()

        await bus.publish('engine.trade', {
            'price': price, 'size': size,
            'side': aggressor.side,
            'aggressor': aggressor.owner,
            'passive': passive.owner,
            'aggressor_order_id': aggressor.order_id,
            'passive_order_id': passive.order_id,
            'timestamp': trade.timestamp,
        })

        await self._check_stop_orders(price)

        if aggressor.owner == 'user':
            impact = size * price
            direction = 1 if aggressor.side == 'buy' else -1
            await bus.publish('user.impact', {
                'net_qty': size,
                'direction': direction,
                'impact_magnitude': impact,
            })

        return trade

    async def _check_stop_orders(self, last_price: float):
        triggered = []
        remaining_stops = []

        for stop in self.stop_orders:
            should_trigger = False
            if stop.side == 'sell' and last_price <= stop.stop_price:
                should_trigger = True
            elif stop.side == 'buy' and last_price >= stop.stop_price:
                should_trigger = True

            if should_trigger:
                triggered.append(stop)
            else:
                remaining_stops.append(stop)

        self.stop_orders = remaining_stops

        if self._matching_depth > 0 and triggered:
            # Defer stop processing to avoid reentrancy corruption
            self._deferred_stops.append((last_price, triggered))
        else:
            for stop in triggered:
                stop.type = 'market'
                await self.submit_order(stop)

    def get_snapshot(self, depth: int = 50) -> dict:
        asks = []
        for price in list(self.asks.keys())[:depth]:
            total = sum(o.size - o.filled for o in self.asks[price])
            if total > 0:
                asks.append([round(price, 2), round(total, 3)])

        bids = []
        for price in list(self.bids.keys())[:depth]:
            total = sum(o.size - o.filled for o in self.bids[price])
            if total > 0:
                bids.append([round(price, 2), round(total, 3)])

        if len(asks) >= 5 and len(bids) >= 5:
            self._last_snapshot = {'asks': asks, 'bids': bids, 'mid': self.mid_price}

        return self._last_snapshot

    def _update_mid(self):
        best_ask = next(iter(self.asks), None)
        best_bid = next(iter(self.bids), None)
        if best_ask and best_bid:
            new_mid = (best_ask + best_bid) / 2
            if new_mid <= 0:
                print(f"[Engine] WARNING: negative mid_price={new_mid:.2f} "
                      f"from best_ask={best_ask:.2f} best_bid={best_bid:.2f} — rejected")
                return
            # Sanity-check against the cold-start anchor (fixed reference point).
            if self.anchor_price > 0:
                if new_mid > self.anchor_price * 2 or new_mid < self.anchor_price * 0.1:
                    print(f"[Engine] WARNING: mid {new_mid:.2f} too far from anchor "
                          f"{self.anchor_price:.2f} (best_ask={best_ask:.2f} best_bid={best_bid:.2f}) — rejected")
                    return
            self.mid_price = new_mid
            self.last_mid_update_time = time.monotonic()
