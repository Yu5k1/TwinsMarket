import uuid
import numpy as np
from engine import Order, MatchingEngine
from event_bus import bus


class TrendFollower:
    def __init__(self, engine: MatchingEngine):
        self.engine = engine
        self.price_history: list[float] = []
        self.positions: list[dict] = []
        self.max_inventory = 2000.0
        self.inventory = 0.0

        bus.subscribe('oracle.tick', self._on_oracle_tick)
        bus.subscribe('oracle.shock', self._on_shock)

    async def _on_oracle_tick(self, data):
        self.price_history.append(data['index_price'])
        if len(self.price_history) > 100:
            self.price_history.pop(0)

    async def _on_shock(self, data):
        if data['is_large'] and abs(self.inventory) < self.max_inventory:
            side = 'buy' if data['direction'] > 0 else 'sell'
            size = 100.0
            ref_price = self.price_history[-1] if self.price_history else 0.0
            if ref_price <= 0:
                return
            stop_price = ref_price * (0.98 if side == 'buy' else 1.02)

            order = Order(
                order_id=str(uuid.uuid4()), owner='trend_follower',
                side=side, type='market', price=0.0, size=size
            )
            trades = await self.engine.submit_order(order)
            for t in trades:
                if side == 'buy':
                    self.inventory += t.size
                else:
                    self.inventory -= t.size
                stop = Order(
                    order_id=str(uuid.uuid4()), owner='trend_follower',
                    side='sell' if side == 'buy' else 'buy',
                    type='stop', price=0.0, size=t.size,
                    stop_price=stop_price
                )
                await self.engine.submit_order(stop)

    async def tick(self):
        if getattr(self, 'startup_delay_ticks', 0) > 0:
            self.startup_delay_ticks -= 1
            return

        if len(self.price_history) < 20:
            return

        ref_price = self.price_history[-1]
        if ref_price <= 0:
            return

        recent_high = max(self.price_history[-20:])
        recent_low = min(self.price_history[-20:])

        breakout_up = ref_price > recent_high * 1.001
        breakout_down = ref_price < recent_low * 0.999

        if breakout_up and self.inventory < self.max_inventory:
            size = np.random.uniform(20, 80)
            stop_price = ref_price * 0.985
            order = Order(
                order_id=str(uuid.uuid4()), owner='trend_follower',
                side='buy', type='limit',
                price=round(ref_price * 1.001, 2), size=round(size, 3)
            )
            trades = await self.engine.submit_order(order)
            for t in trades:
                self.inventory += t.size
                stop = Order(
                    order_id=str(uuid.uuid4()), owner='trend_follower',
                    side='sell', type='stop', price=0.0,
                    size=t.size, stop_price=stop_price
                )
                await self.engine.submit_order(stop)

        elif breakout_down and self.inventory > -self.max_inventory:
            size = np.random.uniform(20, 80)
            stop_price = ref_price * 1.015
            order = Order(
                order_id=str(uuid.uuid4()), owner='trend_follower',
                side='sell', type='limit',
                price=round(ref_price * 0.999, 2), size=round(size, 3)
            )
            trades = await self.engine.submit_order(order)
            for t in trades:
                self.inventory -= t.size
                stop = Order(
                    order_id=str(uuid.uuid4()), owner='trend_follower',
                    side='buy', type='stop', price=0.0,
                    size=t.size, stop_price=stop_price
                )
                await self.engine.submit_order(stop)
