import asyncio
import math
import time
import numpy as np
from event_bus import bus


class Oracle:
    """Synthetic price model — log-return formulation.

    Price evolution per tick (100 ms):
        log_return  = momentum_contrib  +  jump_pct  +  micro_noise_pct
        log_return  = clamp(log_return, ±2 %)
        index_price = prev_index_price * exp(log_return)

    `mu` and `theta` are deterministic OU bookkeeping levels used only by the
    anchor watchdog and `momentum_decay` term; they do NOT inject random noise
    into price (random magnitude lives entirely in the log-return components).

    GARCH(1,1) `sigma_t` is exported as a market-stress signal that downstream
    consumers (market makers, news system) use to size spreads. It is no
    longer wired into log_return.
    """

    # ── GARCH(1,1) parameters, calibrated for 100ms tick frequency ─────────
    # Steady-state: sigma2 = omega / (1 - alpha - beta) = 4.5e-10 / 0.005 = 9e-8
    # → sigma_t ≈ 0.0003 (≈ 0.03% per 100ms tick)
    GARCH_OMEGA = 4.5e-10
    GARCH_ALPHA = 0.04
    GARCH_BETA  = 0.955
    SIGMA2_MIN  = 5e-9      # sigma_t ≈ 7e-5
    SIGMA2_MAX  = 4e-6      # sigma_t ≈ 2e-3 (panic cap, ETH ~1.2%/min)

    # Per-tick log-return hard cap — ±2 % is enough to absorb a max jump
    LOG_RETURN_CAP = 0.02

    # Anchor reanchor threshold — tightened from 10 % to 4 % so that the
    # arbitrageur's corrective limit zone (5 %+) and the watchdog overlap.
    ANCHOR_REANCHOR_PCT = 0.04

    # Micro-noise per-tick std (log-return units). Yields ~0.24 %/min wander
    # under pure noise alone, before momentum / jumps.
    MICRO_NOISE_STD = 0.0001

    # Theta slow random-walk cadence (in oracle ticks): 6000 × 100ms = 10 min
    THETA_WALK_TICKS = 6000
    THETA_WALK_STD = 0.0005     # ±0.05 % per 10 min
    THETA_BOUND_LOW = 0.30      # theta clipped to [seed×0.3, seed×3.0]
    THETA_BOUND_HIGH = 3.00

    # Force-trend cooling window after any jump (ticks)
    FORCE_TREND_TICKS = 30

    def __init__(self, init_price: float = 23500.0):
        self.index_price = init_price
        self.mu = init_price
        self.theta = init_price
        self.seed_price = init_price            # frozen long-term theta anchor

        self.trend_bias = 0.0
        self.recent_return = 0.0
        self.sigma_t = math.sqrt(9e-8)          # ≈ 3e-4
        self.sigma2 = self.sigma_t ** 2

        self.market_state = 'calm'
        self.state_timer = 0

        # OU parameters
        self.kappa = 0.002
        # sigma_ou retained as a preset knob (apply_volatility_mult), but no
        # longer injected into mu — kept so presets that scale it remain valid.
        self.sigma_ou = 0.0008

        # Momentum parameters
        self.momentum_alpha = 0.82
        self.momentum_beta = 0.08

        # External feedback: latest funding rate (written by FundingRateCalculator)
        self.last_funding_rate = 0.0

        # Forced post-jump state cooler
        self._force_state = None
        self._force_state_ticks = 0

        # Theta slow random-walk counter
        self._theta_update_counter = 0

        # Market state transition matrix — panic retention reduced 0.40 → 0.25
        self.transition = {
            'calm':     {'calm': 0.970, 'trend': 0.020, 'volatile': 0.008, 'panic': 0.002},
            'trend':    {'calm': 0.050, 'trend': 0.920, 'volatile': 0.025, 'panic': 0.005},
            'volatile': {'calm': 0.115, 'trend': 0.155, 'volatile': 0.715, 'panic': 0.015},
            'panic':    {'calm': 0.250, 'trend': 0.150, 'volatile': 0.350, 'panic': 0.250},
        }
        self.shock_recovery_ticks = 0

        # State multipliers (jump frequency / momentum amplification per state)
        self.state_vol_mult = {'calm': 0.5, 'trend': 1.0, 'volatile': 2.0, 'panic': 4.0}
        self.state_jump_mult = {'calm': 0.3, 'trend': 0.8, 'volatile': 1.5, 'panic': 3.0}
        self.state_momentum_mult = {'calm': 0.5, 'trend': 1.5, 'volatile': 0.8, 'panic': 0.3}

    def apply_volatility_mult(self, mult: float):
        """Used by presets to scale OU noise — kept for backward compat with
        presets even though OU noise itself is no longer injected into mu."""
        self.sigma_ou *= mult

    def set_initial_state(self, state: str):
        if state in self.transition:
            self.market_state = state

    async def tick(self):
        # 0. Anchor watchdog — pull mu/theta back if they wander >4 % from
        #    index_price. This overlaps with the arbitrageur's corrective
        #    zone so anchors and orderbook can never drift independently.
        if self.index_price > 100:
            if abs(self.mu - self.index_price) / self.index_price > self.ANCHOR_REANCHOR_PCT:
                self.mu = self.index_price
            if abs(self.theta - self.index_price) / self.index_price > self.ANCHOR_REANCHOR_PCT:
                self.theta = self.index_price

        # 1. GARCH(1,1) volatility update — feeds MM spread widening
        r = self.recent_return
        self.sigma2 = (self.GARCH_OMEGA
                       + self.GARCH_ALPHA * r * r
                       + self.GARCH_BETA * self.sigma2)
        self.sigma2 = max(self.SIGMA2_MIN, min(self.sigma2, self.SIGMA2_MAX))
        self.sigma_t = math.sqrt(self.sigma2)

        # 2. OU process on mu — DETERMINISTIC drift only
        ou_drift = self.kappa * (self.theta - self.mu)
        self.mu += ou_drift

        # 3. Momentum (in log-return units, with distance-to-theta decay)
        deviation = abs(self.index_price - self.theta) / self.theta if self.theta > 0 else 0.0
        momentum_decay = max(0.0, 1.0 - deviation * 10)
        self.trend_bias = (self.momentum_alpha * self.trend_bias
                           + self.momentum_beta * self.recent_return)
        self.trend_bias *= momentum_decay
        momentum_contrib = self.trend_bias * self.state_momentum_mult[self.market_state]

        # 4. Jump process → log-return percentage
        jump_pct = 0.0
        jump_lambda = 0.0005 * self.state_jump_mult[self.market_state]
        if np.random.random() < jump_lambda:
            jump_pct = self._generate_jump()

        # 5. Micro noise → log-return percentage
        micro_noise_pct = np.random.randn() * self.MICRO_NOISE_STD

        # 6. Combine into log-return + hard cap, then multiplicative update
        log_return = momentum_contrib + jump_pct + micro_noise_pct
        log_return = max(-self.LOG_RETURN_CAP, min(self.LOG_RETURN_CAP, log_return))

        prev_price = self.index_price
        self.index_price = self.index_price * math.exp(log_return)
        self.index_price = max(self.index_price, 100.0)

        # 7. Update recent_return for next tick's GARCH input
        self.recent_return = (self.index_price - prev_price) / prev_price

        # 8. State machine evaluation (every 24 ticks ≈ 2.4 s)
        self.state_timer += 1
        if self.state_timer >= 24:
            self.state_timer = 0
            self._update_state()

        # 9. Theta slow random-walk (every ~10 min) — gives the simulator a
        #    slow drift envelope for multi-day sessions.
        self._theta_update_counter += 1
        if self._theta_update_counter >= self.THETA_WALK_TICKS:
            self._theta_update_counter = 0
            theta_drift = np.random.randn() * self.THETA_WALK_STD
            self.theta *= math.exp(theta_drift)
            self.theta = max(self.seed_price * self.THETA_BOUND_LOW,
                             min(self.seed_price * self.THETA_BOUND_HIGH, self.theta))

        # 10. Broadcast
        await bus.publish('oracle.tick', {
            'index_price': self.index_price,
            'volatility': self.sigma_t,
            'market_state': self.market_state,
            'timestamp': time.time(),
        })

    def _generate_jump(self) -> float:
        # Funding-rate-driven down-bias: long crowding (rate>0) → leans down.
        # Bias bounded to 0.43..0.57 so even max funding can't fully one-side.
        down_bias = 0.50 + max(-0.07, min(0.07, self.last_funding_rate * 8))
        is_down = np.random.random() < down_bias
        is_large = np.random.random() < 0.06

        if is_large:
            magnitude = np.random.uniform(0.01, 0.04)
        else:
            magnitude = np.random.uniform(0.002, 0.008)

        direction = -1 if is_down else 1
        jump_pct = direction * magnitude

        # Force trend state for 30 ticks (3 s) after any jump — caps the
        # panic→panic self-reinforcement that retention-matrix tweaks alone
        # could not suppress.
        self._force_state = 'trend'
        self._force_state_ticks = self.FORCE_TREND_TICKS
        self.shock_recovery_ticks = 24

        asyncio.create_task(bus.publish('oracle.shock', {
            'direction': direction,
            'magnitude': magnitude,
            'is_large': is_large,
            'timestamp': time.time(),
        }))
        return jump_pct

    def _update_state(self):
        # Honour forced state (post-jump cooling) before normal evaluation
        if self._force_state_ticks > 0:
            self.market_state = self._force_state
            self._force_state_ticks -= 1
            return

        if self.shock_recovery_ticks > 0:
            self.shock_recovery_ticks -= 1
            recovery_boost = {'calm': 0.05, 'trend': 0.05, 'volatile': 0.0, 'panic': -0.10}
            trans = dict(self.transition[self.market_state])
            for k, v in recovery_boost.items():
                trans[k] = max(0.0, trans[k] + v)
            total = sum(trans.values())
            if total > 0:
                trans = {k: v / total for k, v in trans.items()}
        else:
            trans = self.transition[self.market_state]

        states = list(trans.keys())
        probs = list(trans.values())
        self.market_state = np.random.choice(states, p=probs)

    def apply_user_impact(self, net_qty: float, direction: int):
        """User large-order impact feeds into momentum (still in percentage)."""
        impact = (net_qty * self.index_price) / 1e8
        impact = min(impact, 0.0002)
        self.trend_bias += direction * impact

    def generate_historical_path(self, n_steps: int, start_price: float) -> list[float]:
        """Cold-start synthetic price path. Kept for backward compat with any
        callers; the production cold-start in main.py uses its own GBM."""
        prices = []
        px = start_price
        for i in range(n_steps):
            noise = np.random.randn() * 0.0003 * px
            momentum = 0.0
            if i > 0:
                momentum = 0.3 * (px - prices[-1])
            px = px + noise + momentum
            px = max(px, 100.0)
            prices.append(px)
        return prices
