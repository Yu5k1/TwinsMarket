import time


class Faucet:
    REPLENISH_AMOUNT = 500_000.0
    TRIGGER_THRESHOLD = 100_000.0
    COOLDOWN_SECONDS = 600

    def __init__(self, position_manager):
        self.pm = position_manager
        self.last_used = 0.0

    @property
    def cooldown_remaining(self) -> int:
        if self.last_used == 0:
            return 0
        elapsed = time.time() - self.last_used
        return max(0, int(self.COOLDOWN_SECONDS - elapsed))

    def should_show(self) -> bool:
        return self.pm.balance < self.TRIGGER_THRESHOLD

    async def claim(self) -> dict:
        now = time.time()
        if now - self.last_used < self.COOLDOWN_SECONDS:
            remaining = int(self.COOLDOWN_SECONDS - (now - self.last_used))
            return {'success': False, 'cooldown_remaining': remaining}

        self.pm.balance += self.REPLENISH_AMOUNT
        self.last_used = now
        return {
            'success': True,
            'amount': self.REPLENISH_AMOUNT,
            'new_balance': self.pm.balance,
        }
