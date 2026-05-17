import math
import uuid
import time
import numpy as np
from engine import Order, MatchingEngine
from event_bus import bus


class MarketMaker:
    STARTUP_BASE = 1.0
    STARTUP_AMPL = 1.5
    STARTUP_TAU  = 7.0

    def __init__(self, name: str, config: dict, engine: MatchingEngine, oracle=None):
        self.name = name
        self.cfg = config
        self.engine = engine
        self.oracle = oracle
        self.inventory = 0.0
        self.active_orders: dict[str, Order] = {}
        self.last_index_price = 0.0
        self.current_volatility = 0.001
        self.market_state = 'calm'
        self.is_shocked = False
        self.shock_cooldown = 0
        self.pending_reposts: list[tuple[float, Order]] = []
        self._startup_time: float | None = None
        self.startup_spread_mult = 1.0
        self._filled_as_passive = 0.0

        bus.subscribe('oracle.tick', self._on_oracle_tick)
        bus.subscribe('oracle.shock', self._on_shock)
        bus.subscribe('engine.trade', self._on_trade)

    async def _on_oracle_tick(self, data: dict):
        self.last_index_price = data['index_price']
        self.current_volatility = data['volatility']
        self.market_state = data['market_state']

    async def _on_shock(self, data: dict):
        if self.cfg.get('cancel_on_shock', True):
            await self._cancel_all()
        self.is_shocked = True
        self.shock_cooldown = self.cfg.get('shock_cooldown_ticks', 3)

    async def _on_trade(self, data: dict):
        pass

    async def tick(self):
        await self._process_reposts()

        if self.is_shocked:
            self.shock_cooldown -= 1
            if self.shock_cooldown <= 0:
                self.is_shocked = False
                await self._post_quotes(spread_multiplier=2.0)
            return

        if not self.oracle or self.oracle.index_price <= 0:
            return

        if abs(self.inventory) >= self.cfg['max_inventory']:
            await self._emergency_hedge()
            return

        await self._post_quotes()

    def _sample_size(self) -> float:
        """Sample a single order size using lognormal distribution from config."""
        mu    = self.cfg.get('size_lognormal_mean',  1.6)
        sigma = self.cfg.get('size_lognormal_sigma', 0.8)
        lo    = self.cfg.get('size_min', 0.1)
        hi    = self.cfg.get('size_max', 3.0)
        return float(np.clip(np.random.lognormal(mu, sigma), lo, hi))

    async def _post_quotes(self, spread_multiplier: float = 1.0):
        # Incremental update: compute desired orders first, then only cancel
        # and replace levels whose price has shifted by more than 1 tick.
        # This avoids the full-book flash that _cancel_all() caused every tick.

        mid = self.oracle.index_price
        cfg = self.cfg

        vol_mult = min(1.0 + self.current_volatility * 500, 3.0)
        state_mult = {'calm': 0.7, 'trend': 1.0, 'volatile': 1.8, 'panic': 3.5}[self.market_state]
        startup_mult = self._compute_startup_mult()
        half_spread = (mid * cfg['base_spread_pct'] / 2
                       * vol_mult * state_mult * spread_multiplier * startup_mult)

        inv_ratio = max(-1.0, min(self.inventory / cfg['max_inventory'], 1.0))
        skew = inv_ratio * cfg['skew_factor'] * mid
        bid_base = mid - half_spread - skew
        ask_base = mid + half_spread - skew

        MIN_TICK = 0.01
        bid_base = max(bid_base, mid * 0.5)
        ask_base = max(ask_base, bid_base + MIN_TICK, mid * 0.5 + MIN_TICK)

        ofi = self.engine.ofi
        ofi_adj = 0.0
        if ofi > cfg['ofi_sensitivity']:
            ofi_adj = (ofi - cfg['ofi_sensitivity']) * 0.5
        elif ofi < (1 - cfg['ofi_sensitivity']):
            ofi_adj = -(cfg['ofi_sensitivity'] - ofi) * 0.5

        desired: list[Order] = []
        for i in range(cfg['n_levels']):
            spacing = cfg.get('level_spacing_usdt',
                              mid * cfg.get('level_spacing_inner', 0.0001))

            size = self._sample_size()

            bid_price = round(bid_base - i * spacing, 2)
            ask_price = round(ask_base + i * spacing, 2)

            bid_size = size * (1 - max(0, ofi_adj))
            ask_size = size * (1 + max(0, ofi_adj))

            if bid_size > 0.01 and bid_price > 0.01:
                desired.append(Order(
                    order_id=str(uuid.uuid4()), owner=self.name,
                    side='buy', type='limit',
                    price=bid_price, size=round(bid_size, 3)
                ))
            if ask_size > 0.01 and ask_price > bid_price:
                desired.append(Order(
                    order_id=str(uuid.uuid4()), owner=self.name,
                    side='sell', type='limit',
                    price=ask_price, size=round(ask_size, 3)
                ))

        # Match active orders against desired: keep those within 1 tick
        matched_desired: set[int] = set()
        to_cancel: list[str] = []

        for oid, active in list(self.active_orders.items()):
            best_idx: int = -1
            best_dist: float = float('inf')
            for idx, d in enumerate(desired):
                if idx in matched_desired:
                    continue
                if d.side != active.side:
                    continue
                dist = abs(d.price - active.price)
                if dist < best_dist:
                    best_dist = dist
                    best_idx = idx

            if best_idx >= 0 and best_dist <= MIN_TICK:
                matched_desired.add(best_idx)
            else:
                to_cancel.append(oid)

        for oid in to_cancel:
            order = self.active_orders.pop(oid, None)
            if order is None:
                continue
            order.status = 'cancelled'
            self._track_inventory_from_fill(order)
            self.engine._remove_order(order)

        for idx, order in enumerate(desired):
            if idx in matched_desired:
                continue
            trades = await self.engine.submit_order(order)
            self.active_orders[order.order_id] = order
            for t in trades:
                if t.passive == self.name:
                    delta = t.size if t.side == 'sell' else -t.size
                    self.inventory += delta

    async def _cancel_all(self):
        for oid, order in list(self.active_orders.items()):
            order.status = 'cancelled'
            self._track_inventory_from_fill(order)
            self.engine._remove_order(order)
        self.active_orders.clear()

    def _track_inventory_from_fill(self, order: Order):
        if order.filled > 0 and order.side == 'sell':
            self.inventory -= order.filled
        elif order.filled > 0 and order.side == 'buy':
            self.inventory += order.filled

    def _compute_startup_mult(self) -> float:
        if self._startup_time is None:
            return self.startup_spread_mult
        elapsed = max(0.0, time.monotonic() - self._startup_time)
        return self.STARTUP_BASE + self.STARTUP_AMPL * math.exp(-elapsed / self.STARTUP_TAU)

    async def _process_reposts(self):
        now = time.time()
        self.pending_reposts = [(rt, o) for rt, o in self.pending_reposts if now < rt]

    async def _emergency_hedge(self):
        await self._cancel_all()
        mid = self.oracle.index_price
        cfg = self.cfg
        hedge_size = round(self._sample_size() * 1.5, 3)
        if self.inventory > 0:
            for i in range(cfg['n_levels']):
                spacing = cfg.get('level_spacing_usdt',
                                  mid * cfg.get('level_spacing_inner', 0.0001))
                order = Order(
                    order_id=str(uuid.uuid4()), owner=self.name,
                    side='sell', type='limit',
                    price=round(mid + i * spacing, 2),
                    size=hedge_size,
                )
                await self.engine.submit_order(order)
                self.active_orders[order.order_id] = order
        else:
            for i in range(cfg['n_levels']):
                spacing = cfg.get('level_spacing_usdt',
                                  mid * cfg.get('level_spacing_inner', 0.0001))
                order = Order(
                    order_id=str(uuid.uuid4()), owner=self.name,
                    side='buy', type='limit',
                    price=round(mid - i * spacing, 2),
                    size=hedge_size,
                )
                await self.engine.submit_order(order)
                self.active_orders[order.order_id] = order