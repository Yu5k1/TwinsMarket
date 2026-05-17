import uuid
from engine import Order
from event_bus import bus


class LiquidationMonitor:
    def __init__(self, engine, positions):
        self.engine = engine
        self.positions = positions
        self.insurance_fund = 50000.0

        bus.subscribe('market.funding', self._on_funding)
        bus.subscribe('engine.trade', self._on_trade)

    async def tick(self):
        mark_price = self.positions.mark_price
        if mark_price == 0:
            return

        for pos in self.positions.get_all():
            liq_price = self.positions.liquidation_price(pos)

            should_liquidate = (
                (pos['side'] == 'long' and mark_price <= liq_price) or
                (pos['side'] == 'short' and mark_price >= liq_price)
            )

            if should_liquidate:
                await self._liquidate(pos, mark_price)

    async def _liquidate(self, pos: dict, mark_price: float):
        close_side = 'sell' if pos['side'] == 'long' else 'buy'
        liq_order = Order(
            order_id=str(uuid.uuid4()),
            owner='liquidation_engine',
            side=close_side, type='market',
            price=0.0, size=pos['size']
        )
        trades = await self.engine.submit_order(liq_order)

        if not trades:
            return

        avg_fill = sum(t.price * t.size for t in trades) / sum(t.size for t in trades)
        position_value = pos['size'] * pos['entry_price']

        # PnL relative to entry: if long and fill below entry, loss = size * (entry - fill)
        if pos['side'] == 'long':
            pnl_per_unit = avg_fill - pos['entry_price']
        else:
            pnl_per_unit = pos['entry_price'] - avg_fill
        trading_pnl = pnl_per_unit * pos['size']

        liq_fee = pos['size'] * avg_fill * 0.005
        total_loss = -trading_pnl + liq_fee  # negative trading_pnl means loss

        margin = pos['margin']
        if total_loss <= margin:
            net_return = margin - total_loss
            await self.positions.close_position(pos['id'], net_return)
        else:
            deficit = total_loss - margin
            await self.positions.close_position(pos['id'], 0)

            if self.insurance_fund >= deficit:
                self.insurance_fund -= deficit
            else:
                deficit -= self.insurance_fund
                self.insurance_fund = 0
                await bus.publish('insurance.depleted', {'deficit': deficit})
                await self._trigger_adl(pos, deficit)

        await bus.publish('engine.liquidation', {
            'position_id': pos['id'],
            'user_id': pos['user_id'],
            'side': pos['side'],
            'price': avg_fill,
            'size': pos['size'],
        })

    async def _trigger_adl(self, liquidated_pos: dict, deficit: float):
        target_side = 'short' if liquidated_pos['side'] == 'long' else 'long'
        all_positions = self.positions.get_all()

        profitable = sorted(
            [p for p in all_positions
             if p['side'] == target_side and p['unrealized_pnl'] > 0],
            key=lambda p: p['unrealized_pnl'],
            reverse=True
        )

        remaining_deficit = deficit
        mid = self.engine.mid_price
        if mid == 0:
            return

        for pos in profitable:
            if remaining_deficit <= 0:
                break
            reduce_size = min(pos['size'], remaining_deficit / mid)
            close_side = 'sell' if pos['side'] == 'long' else 'buy'
            adl_order = Order(
                order_id=str(uuid.uuid4()),
                owner='adl_engine',
                side=close_side, type='market',
                price=0.0, size=round(reduce_size, 3)
            )
            await self.engine.submit_order(adl_order)
            remaining_deficit -= reduce_size * mid

    async def _on_funding(self, data):
        await self.positions.settle_funding(data['rate'])

    async def _on_trade(self, data):
        fee_contribution = data['price'] * data['size'] * 0.0001
        self.insurance_fund += fee_contribution
