import json
import time
import numpy as np
from fastapi import WebSocket, WebSocketDisconnect
from event_bus import bus


def _build_pending(pending_ref, engine):
    """Build pending orders list from _pending_user_orders dict + engine state."""
    if not pending_ref or not engine:
        return []
    result = []
    for oid, meta in pending_ref.items():
        order = engine.order_map.get(oid)
        if order is None or order.status in ('filled', 'cancelled'):
            continue
        result.append({
            'order_id': oid,
            'side': order.side,
            'price': order.price,
            'size': order.size,
            'filled': order.filled,
            'leverage': meta.get('leverage', 1),
            'status': order.status,
        })
    return result


def _to_native(obj):
    """Recursively convert numpy types to Python native types for JSON serialization."""
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return float(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    if isinstance(obj, np.ndarray):
        return [_to_native(x) for x in obj.tolist()]
    if isinstance(obj, dict):
        return {k: _to_native(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_native(x) for x in obj]
    return obj


class WSPushManager:
    def __init__(self, engine, position_manager, funding_calc,
                 liquidation_monitor, news_system, oracle, faucet,
                 trade_journal=None, pending_orders_ref=None):
        self.engine = engine
        self.position_manager = position_manager
        self.funding_calc = funding_calc
        self.liquidation_monitor = liquidation_monitor
        self.news_system = news_system
        self.oracle = oracle
        self.faucet = faucet
        self.trade_journal = trade_journal
        self.pending_orders_ref = pending_orders_ref
        self.connections: list[WebSocket] = []

        # Recent trades buffer for new connections
        self.recent_trades: list[dict] = []

        # Log entries for frontend display
        self.log_entries: list[str] = []

        bus.subscribe('engine.trade', self._on_trade)
        bus.subscribe('engine.liquidation', self._on_liquidation)
        bus.subscribe('insurance.depleted', self._on_insurance_depleted)
        bus.subscribe('market.funding', self._on_funding_event)
        bus.subscribe('oracle.shock', self._on_shock)
        bus.subscribe('oracle.tick', self._on_state_change)
        bus.subscribe('user.impact', self._on_user_impact)
        bus.subscribe('order.rejected', self._on_order_rejected)

    async def _on_trade(self, data):
        self.recent_trades.append(data)
        if len(self.recent_trades) > 200:
            self.recent_trades.pop(0)

    async def _on_liquidation(self, data):
        ts = time.strftime('%H:%M:%S', time.localtime())
        self.log_entries.append(
            f"[{ts}] \U0001f4c9 清算：仓位 {data['position_id'][:8]} {data['side']} "
            f"强平价格 {data['price']:.2f}，数量 {data['size']:.3f}"
        )
        if len(self.log_entries) > 100:
            self.log_entries.pop(0)

    async def _on_insurance_depleted(self, data):
        ts = time.strftime('%H:%M:%S', time.localtime())
        self.log_entries.append(
            f"[{ts}] \U0001f534 保险基金耗尽！缺口 {data['deficit']:.2f} USDT，触发 ADL"
        )
        if len(self.log_entries) > 100:
            self.log_entries.pop(0)

    async def _on_funding_event(self, data):
        ts = time.strftime('%H:%M:%S', time.localtime())
        direction = '\U0001f7e2' if data['rate'] > 0 else '\U0001f534'
        self.log_entries.append(
            f"[{ts}] \U0001f4a7 资金费率：{data['rate']*100:.4f}% {direction}，"
            f"下次结算 {time.strftime('%H:%M:%S', time.localtime(data['next_settlement']))}"
        )
        if len(self.log_entries) > 100:
            self.log_entries.pop(0)

    async def _on_shock(self, data):
        ts = time.strftime('%H:%M:%S', time.localtime())
        direction = '↑' if data['direction'] > 0 else '↓'
        label = '大幅' if data['is_large'] else '小幅'
        self.log_entries.append(
            f"[{ts}] ⚡ 价格跳跃 {direction}{label} "
            f"{data['magnitude']*100:.2f}%"
        )
        if len(self.log_entries) > 100:
            self.log_entries.pop(0)

    async def _on_state_change(self, data):
        # Log market state transitions
        pass  # state change logging handled in push loop

    async def _on_order_rejected(self, data):
        if data.get('owner') != 'user':
            return
        ts = time.strftime('%H:%M:%S', time.localtime())
        reason = data.get('reason', 'unknown')
        msg_map = {'no_liquidity': '暂无流动性，请稍后重试'}
        self.log_entries.append(
            f"[{ts}] ⚠️ 订单被拒：{msg_map.get(reason, reason)}"
        )
        if len(self.log_entries) > 100:
            self.log_entries.pop(0)

    async def _on_user_impact(self, data):
        ts = time.strftime('%H:%M:%S', time.localtime())
        direction = '\U0001f4c8' if data['direction'] > 0 else '\U0001f4c9'
        self.log_entries.append(
            f"[{ts}] \U0001f40b 用户大单：{'买入' if data['direction'] > 0 else '卖出'} "
            f"{data['net_qty']:.3f} AEN，冲击量 {data['impact_magnitude']:.0f} USDT"
        )
        if len(self.log_entries) > 100:
            self.log_entries.pop(0)

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.connections.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.connections:
            self.connections.remove(ws)

    async def push_tick(self):
        if not self.connections:
            self.connections = [c for c in self.connections if c.client_state.name == 'CONNECTED']
            return

        # Build the full state snapshot
        orderbook = self.engine.get_snapshot(600)
        tick_data = {
            'type': 'tick',
            'price': self.engine.last_price,
            'mid_price': self.engine.mid_price,
            'mark_price': self.position_manager.mark_price,
            'index_price': self.oracle.index_price,
            'orderbook': orderbook,
            'trades': self.recent_trades[-20:],
            'klines': {
                tf: {
                    'bars': builder.get_bars(200),
                }
                for tf, builder in self.engine.kline_builders.items()
            },
            'funding_rate': self.funding_calc.current_rate,
            'next_funding': self.funding_calc.next_settlement,
            'positions': self.position_manager.get_all(),
            'balance': self.position_manager.balance,
            'insurance_fund': self.liquidation_monitor.insurance_fund,
            'market_state': self.oracle.market_state,
            'news': self.news_system.news_history[:10],
            'faucet': {
                'should_show': self.faucet.should_show(),
                'cooldown': self.faucet.cooldown_remaining,
            },
            'spread': self.engine.spread,
            'ofi': self.engine.ofi,
            'log': self.log_entries[-30:],
            'journal': self.trade_journal.entries[:20] if self.trade_journal else [],
            'pending_orders': _build_pending(self.pending_orders_ref, self.engine),
        }

        payload = json.dumps(_to_native(tick_data))
        dead = []
        for ws in self.connections:
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)
