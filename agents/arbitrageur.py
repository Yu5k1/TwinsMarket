import uuid
import numpy as np
from engine import Order, MatchingEngine
from event_bus import bus


class Arbitrageur:
    """Price anchor agent: arbitrages deviation between mid_price and index_price."""

    def __init__(self, engine: MatchingEngine, oracle=None):
        self.engine = engine
        self.oracle = oracle
        self.index_price = 0.0
        self.funding_rate = 0.0
        self.inventory = 0.0
        self.max_inventory = 3000.0

        self.entry_threshold = 0.0005
        self.market_order_threshold = 0.003

        bus.subscribe('oracle.tick', self._on_oracle_tick)
        bus.subscribe('market.funding', self._on_funding)

    async def _on_oracle_tick(self, data):
        self.index_price = data['index_price']

    async def _on_funding(self, data):
        self.funding_rate = data['rate']

    async def tick(self):
        mid = self.engine.mid_price
        if mid <= 0 or self.index_price <= 0:
            return
        if abs(self.inventory) >= self.max_inventory:
            return

        deviation = (mid - self.index_price) / self.index_price

        # Sanity zone: at ≥5 % gap a market sweep is unsafe (the 16 k → 36 k
        # blowup pattern), but doing nothing also leaves a 5 %–10 % dead zone
        # between us and oracle's 4 % anchor watchdog. Drop one small limit
        # in the index direction to nudge the book without walking it.
        if abs(deviation) >= 0.05:
            side = 'sell' if deviation > 0 else 'buy'
            # Push 0.1 % toward index on the correct side
            corrective_price = self.index_price * (1 - 0.001 if side == 'sell' else 1 + 0.001)
            order = Order(
                order_id=str(uuid.uuid4()), owner='arbitrageur_corrective',
                side=side, type='limit',
                price=round(corrective_price, 2), size=20.0,
            )
            await self.engine.submit_order(order)
            return

        # Price deviation arbitrage
        if abs(deviation) > self.entry_threshold:
            # Cap aggression so size doesn't blow up linearly with the gap.
            aggression = min((abs(deviation) - self.entry_threshold) / self.entry_threshold, 5.0)
            base_size = 50 * (1 + aggression * 3)
            base_size = min(base_size, self.max_inventory - abs(self.inventory))

            if abs(deviation) > self.market_order_threshold:
                # Per-tick cap on market sweeps so we don't walk the book.
                base_size = min(base_size, 300.0)
                side = 'sell' if deviation > 0 else 'buy'
                order = Order(
                    order_id=str(uuid.uuid4()), owner='arbitrageur',
                    side=side, type='market', price=0.0,
                    size=round(base_size, 3)
                )
                trades = await self.engine.submit_order(order)
                delta = sum(t.size for t in trades)
                self.inventory += (-delta if side == 'sell' else delta)
            else:
                side = 'sell' if deviation > 0 else 'buy'
                buffer = 0.0005 * self.index_price
                limit_price = (self.index_price + buffer if side == 'sell'
                               else self.index_price - buffer)
                order = Order(
                    order_id=str(uuid.uuid4()), owner='arbitrageur',
                    side=side, type='limit',
                    price=round(limit_price, 2), size=round(base_size, 3)
                )
                await self.engine.submit_order(order)

        # Funding rate arbitrage
        funding_threshold = 0.0003
        if abs(self.funding_rate) > funding_threshold:
            funding_side = 'sell' if self.funding_rate > 0 else 'buy'
            funding_size = 30.0
            if abs(self.inventory) + funding_size <= self.max_inventory:
                ref_price = self.oracle.index_price if self.oracle and self.oracle.index_price > 0 else mid
                order = Order(
                    order_id=str(uuid.uuid4()), owner='arbitrageur',
                    side=funding_side, type='limit',
                    price=round(ref_price * (0.9998 if funding_side == 'buy' else 1.0002), 2),
                    size=funding_size
                )
                await self.engine.submit_order(order)
