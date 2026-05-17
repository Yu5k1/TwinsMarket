import time
from event_bus import bus


class MarkPriceCalculator:
    WINDOW_SECONDS = 1800  # 30 minutes
    # Reject samples whose |mid - index| / index exceeds this — tightened from
    # 20 % to 5 % so transient single-tick blowups can't poison the TWAP.
    MID_FILTER_THRESHOLD = 0.05
    # A mid that hasn't been refreshed for this long is considered stale; in
    # that window we sample index_price instead of the frozen mid.
    MID_STALE_SECONDS = 0.5

    def __init__(self, engine):
        self.engine = engine
        self.index_price = 0.0
        self.mid_history: list[tuple[float, float]] = []
        self.mark_price = 0.0

        bus.subscribe('oracle.tick', self._on_oracle_tick)

    async def _on_oracle_tick(self, data):
        self.index_price = data['index_price']
        now = time.time()
        mid = self.engine.mid_price
        mid_age = time.monotonic() - getattr(self.engine, 'last_mid_update_time', 0.0)
        mid_is_fresh = mid_age < self.MID_STALE_SECONDS

        if (mid > 0
                and mid_is_fresh
                and (self.index_price <= 0
                     or abs(mid - self.index_price) / self.index_price < self.MID_FILTER_THRESHOLD)):
            self.mid_history.append((now, mid))
        elif not mid_is_fresh and self.index_price > 0:
            # Mid is frozen (single-sided book or matching engine idle).
            # Substitute index so the TWAP keeps tracking reality.
            self.mid_history.append((now, self.index_price))
        # else: mid is fresh but too far from index → drop the sample entirely

        cutoff = now - self.WINDOW_SECONDS
        self.mid_history = [(t, p) for t, p in self.mid_history if t > cutoff]

    def compute(self) -> float:
        if not self.mid_history or self.index_price <= 0:
            return self.index_price

        now = time.time()
        weighted_sum = 0.0
        weight_total = 0.0
        for ts, px in self.mid_history:
            w = 1.0 - (now - ts) / self.WINDOW_SECONDS
            weighted_sum += px * w
            weight_total += w

        twap = weighted_sum / weight_total if weight_total > 0 else self.index_price
        # Clamp basis to ±0.5% of index — prevents a wild TWAP from de-anchoring
        # mark_price (which would otherwise mis-trigger liquidations).
        basis = twap - self.index_price
        max_basis = self.index_price * 0.005
        basis = max(-max_basis, min(basis, max_basis))
        self.mark_price = self.index_price + basis
        return self.mark_price
