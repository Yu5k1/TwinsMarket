import uuid
import time
import numpy as np
from engine import Order, MatchingEngine
from event_bus import bus


class NoiseTrader:
    def __init__(self, engine: MatchingEngine):
        self.engine = engine
        self.positions: list[dict] = []
        self.last_index_price = 0.0
        self.current_volatility = 0.001
        self.market_state = 'calm'
        self.base_rate_mult = 1.0  # Set by preset

        bus.subscribe('oracle.tick', self._on_oracle_tick)

    async def _on_oracle_tick(self, data):
        self.last_index_price = data['index_price']
        self.current_volatility = data['volatility']
        self.market_state = data['market_state']

    async def tick(self):
        if getattr(self, 'startup_delay_ticks', 0) > 0:
            self.startup_delay_ticks -= 1
            return

        ref_price = self.last_index_price
        if ref_price <= 0:
            return

        await self._manage_positions(ref_price)

        base_rate = {'calm': 1.5, 'trend': 2.0, 'volatile': 2.0, 'panic': 3.5}[self.market_state]
        base_rate *= self.base_rate_mult
        vol_boost = 1 + min(self.current_volatility * 200, 2.0)
        expected_orders = base_rate * vol_boost
        n_orders = min(np.random.poisson(expected_orders), 6)

        for _ in range(n_orders):
            await self._place_random_order(ref_price)

    async def _place_random_order(self, ref_price: float):
        ofi = self.engine.ofi
        recent_ret = 0.0

        random_component = np.random.randn() * 0.4
        momentum_component = np.sign(recent_ret) * min(abs(recent_ret) * 100, 1.0) * 0.35
        herding_component = (ofi - 0.5) * 2 * 0.25

        signal = random_component + momentum_component + herding_component

        threshold = 0.3
        if signal > threshold:
            side = 'buy'
        elif signal < -threshold:
            side = 'sell'
        else:
            return

        is_market = np.random.random() < 0.55
        size = np.random.lognormal(mean=0.5, sigma=1.2)
        # Shrink per-order size when the market is stressed — fewer, smaller
        # noise sweeps in volatile/panic, so MMs can keep up with quoting.
        state_size_mult = {'calm': 1.0, 'trend': 1.0,
                           'volatile': 0.6, 'panic': 0.3}[self.market_state]
        size = round(max(0.01, min(size * state_size_mult, 50.0)), 3)

        if is_market:
            order = Order(
                order_id=str(uuid.uuid4()), owner='noise',
                side=side, type='market', price=0.0, size=size
            )
        else:
            offset = np.random.uniform(0.5, 3.0)
            lim_price = (ref_price - offset) if side == 'buy' else (ref_price + offset)
            order = Order(
                order_id=str(uuid.uuid4()), owner='noise',
                side=side, type='limit',
                price=round(lim_price, 2), size=size
            )

        trades = await self.engine.submit_order(order)

        for t in trades:
            if t.aggressor == 'noise':
                self.positions.append({
                    'side': side,
                    'entry_price': t.price,
                    'size': t.size,
                    'open_time': time.time(),
                })

    async def _manage_positions(self, ref_price: float):
        remaining = []
        for pos in self.positions:
            age = time.time() - pos['open_time']
            pnl_pct = ((ref_price - pos['entry_price']) / pos['entry_price']
                       * (1 if pos['side'] == 'buy' else -1))

            should_close = False
            if pnl_pct < -0.02:
                should_close = True
            elif pnl_pct > 0.03:
                should_close = True
            elif age > 300:
                should_close = True
            elif np.random.random() < 0.002:
                should_close = True

            if should_close:
                close_side = 'sell' if pos['side'] == 'buy' else 'buy'
                order = Order(
                    order_id=str(uuid.uuid4()), owner='noise',
                    side=close_side, type='market',
                    price=0.0, size=pos['size']
                )
                await self.engine.submit_order(order)
            else:
                remaining.append(pos)

        self.positions = remaining
