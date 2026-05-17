import math
import time
from event_bus import bus


class FundingRateCalculator:
    MAX_RATE = 0.0075
    BASE_RATE = 0.0001
    # Tick cadence is every 30 s; pick alpha so the EMA half-life is ≈ 8 hours
    # (960 ticks). This keeps individual mid/mark spikes from flipping the
    # funding rate sign every 30 s — a real exchange smooths similarly.
    EMA_ALPHA = 1.0 - math.exp(-math.log(2) / 960.0)   # ≈ 7.22e-4

    def __init__(self, mark_calc, oracle=None):
        self.mark_calc = mark_calc
        # Oracle is optional — when provided the rate is written back so the
        # jump down-bias can lean against crowded long/short funding.
        self.oracle = oracle
        self.current_rate = 0.0
        self.next_settlement = time.time() + 3600
        # EMA of the (mark - index) / index premium; persists across ticks.
        self._ema_premium = 0.0

    async def tick(self):
        index = self.mark_calc.index_price
        mark = self.mark_calc.mark_price
        if index == 0:
            return

        instant_premium = (mark - index) / index
        # EMA smoothing prevents 30s flicker between +0.75% and -0.75%.
        self._ema_premium = ((1.0 - self.EMA_ALPHA) * self._ema_premium
                             + self.EMA_ALPHA * instant_premium)
        premium_index = self._ema_premium

        def clamp(x, lo, hi):
            return max(lo, min(hi, x))

        rate = premium_index + clamp(self.BASE_RATE - premium_index, -0.0005, 0.0005)
        self.current_rate = clamp(rate, -self.MAX_RATE, self.MAX_RATE)

        # Feed the smoothed rate back to the oracle so the jump down-bias
        # responds to crowded positioning.
        if self.oracle is not None:
            self.oracle.last_funding_rate = self.current_rate

        now = time.time()
        if now >= self.next_settlement:
            self.next_settlement = now + 3600
            await bus.publish('market.funding', {
                'rate': self.current_rate,
                'next_settlement': self.next_settlement,
                'timestamp': now,
            })
