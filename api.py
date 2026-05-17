import uuid
from fastapi import FastAPI
from pydantic import BaseModel


app = FastAPI()

# These are injected by main.py at startup
engine = None
position_manager = None
funding_calc = None
liquidation_monitor = None
faucet = None
news_system = None
trade_journal = None

# Track user limit orders resting in the book that haven't filled yet.
# order_id → {leverage, position_id (once first fill creates a position)}
_pending_user_orders: dict[str, dict] = {}


async def _on_user_trade_filled(trade: dict):
    """Called via event bus when any trade fills — creates or updates a user
    position when a user's resting limit order is hit by another trader."""
    if trade.get('passive') != 'user':
        return
    order_id = trade.get('passive_order_id')
    if not order_id or order_id not in _pending_user_orders:
        # This order was placed before the fix was deployed, or is a close order
        return

    meta = _pending_user_orders[order_id]
    fill_price = trade['price']
    fill_size = trade['size']
    pos_id = meta.get('position_id')

    if pos_id and position_manager.positions.get(pos_id):
        # Subsequent fill — update existing position with VWAP
        position_manager.add_fill(pos_id, fill_price, fill_size)
    else:
        # First fill — create position
        pos_side = 'long' if trade['side'] == 'buy' else 'short'
        margin_required = fill_size * fill_price / meta['leverage']
        fee = fill_size * fill_price * position_manager.TAKER_FEE
        if margin_required + fee > position_manager.balance:
            return  # shouldn't happen — balance was checked at order time

        pos = await position_manager.open_position(
            side=pos_side,
            size=fill_size,
            leverage=meta['leverage'],
            entry_price=fill_price,
            order_type='limit',
        )
        if pos:
            meta['position_id'] = pos['id']


def setup_trade_handler():
    """Subscribe to engine trade events to catch fills of user resting orders."""
    from event_bus import bus
    bus.subscribe('engine.trade', _on_user_trade_filled)


class OrderRequest(BaseModel):
    side: str            # 'buy' | 'sell'
    type: str            # 'market' | 'limit'
    price: float = 0.0
    size: float
    leverage: int = 1
    tp_price: float = 0  # optional take-profit trigger
    sl_price: float = 0  # optional stop-loss trigger


class CloseRequest(BaseModel):
    position_id: str
    order_type: str = 'market'


class CancelRequest(BaseModel):
    order_id: str


class TPSLRequest(BaseModel):
    position_id: str
    tp_price: float = 0
    sl_price: float = 0


@app.post('/api/order')
async def place_order(req: OrderRequest):
    if req.leverage < 1 or req.leverage > 25:
        return {'error': 'leverage must be 1-25'}
    if req.size <= 0:
        return {'error': 'size must be positive'}
    if req.side not in ('buy', 'sell'):
        return {'error': 'side must be buy or sell'}
    if req.type not in ('market', 'limit'):
        return {'error': 'type must be market or limit'}
    if req.type == 'limit' and req.price <= 0:
        return {'error': 'limit order requires positive price'}

    mid = engine.mid_price
    if mid == 0:
        return {'error': 'market not ready'}

    # Pre-check balance for the worst-case margin (full size at limit/mid price)
    est_price = req.price if req.type == 'limit' and req.price > 0 else mid
    est_margin = req.size * est_price / req.leverage
    est_fee = req.size * est_price * position_manager.TAKER_FEE
    if est_margin + est_fee > position_manager.balance:
        return {'error': 'insufficient balance'}

    order_id = str(uuid.uuid4())
    pos_side = 'long' if req.side == 'buy' else 'short'

    from engine import Order
    order = Order(
        order_id=order_id, owner='user',
        side=req.side, type=req.type,
        price=req.price, size=req.size,
    )
    trades = await engine.submit_order(order)

    # ── No fills ──
    if not trades:
        if req.type == 'market':
            return {'error': 'no_liquidity'}
        # Resting limit order — store metadata, no position yet
        _pending_user_orders[order_id] = {
            'leverage': req.leverage,
        }
        return {'success': True, 'order_id': order_id, 'status': 'open',
                'message': 'Limit order placed, waiting for fill'}

    # ── Filled (fully or partially) — create position from actual fills ──
    total_filled = sum(t.size for t in trades)
    total_notional = sum(t.size * t.price for t in trades)
    avg_price = total_notional / total_filled if total_filled > 0 else mid

    pos = await position_manager.open_position(
        side=pos_side,
        size=total_filled,
        leverage=req.leverage,
        entry_price=avg_price,
        order_type=req.type,
        tp_price=req.tp_price,
        sl_price=req.sl_price,
    )

    # If partially filled and order is limit, track remainder for later fills
    if order.status == 'partial' and req.type == 'limit':
        _pending_user_orders[order_id] = {
            'leverage': req.leverage,
            'position_id': pos['id'] if pos else None,
        }

    return {'success': True, 'position': pos, 'trades': len(trades),
            'filled_size': total_filled, 'requested_size': req.size}


@app.post('/api/cancel-order')
async def cancel_order(req: CancelRequest):
    if req.order_id not in _pending_user_orders:
        return {'error': 'order not found or already filled'}
    engine.cancel_order(req.order_id)
    _pending_user_orders.pop(req.order_id, None)
    return {'success': True}


@app.post('/api/set-tpsl')
async def set_tpsl(req: TPSLRequest):
    if req.tp_price <= 0 and req.sl_price <= 0:
        return {'error': 'at least one of tp_price or sl_price is required'}
    ok = position_manager.set_tpsl(req.position_id, req.tp_price, req.sl_price)
    if not ok:
        return {'error': 'position not found or invalid TP/SL price'}
    return {'success': True}


@app.post('/api/cancel-tpsl')
async def cancel_tpsl(req: TPSLRequest):
    ok = position_manager.clear_tpsl(req.position_id)
    if not ok:
        return {'error': 'position not found'}
    return {'success': True}


@app.post('/api/close')
async def close_position(req: CloseRequest):
    pos = position_manager.positions.get(req.position_id)
    if not pos:
        return {'error': 'position not found'}

    close_side = 'sell' if pos['side'] == 'long' else 'buy'
    from engine import Order
    order = Order(
        order_id=f"close_{pos['id']}", owner='user',
        side=close_side, type=req.order_type,
        price=0.0, size=pos['size'],
    )
    trades = await engine.submit_order(order)
    if req.order_type == 'market' and not trades:
        return {'success': False, 'error': 'no_liquidity'}

    # Actual PnL from VWAP of filled trades
    total_filled = sum(t.size for t in trades)
    total_notional = sum(t.size * t.price for t in trades)
    vwap_close = total_notional / total_filled if total_filled > 0 else pos['entry_price']

    if pos['side'] == 'long':
        actual_pnl = (vwap_close - pos['entry_price']) * total_filled
    else:
        actual_pnl = (pos['entry_price'] - vwap_close) * total_filled

    close_fee = total_notional * position_manager.TAKER_FEE
    actual_pnl -= close_fee
    close_price = vwap_close

    await position_manager.close_position(pos['id'], pos['margin'] + actual_pnl)

    if trade_journal:
        trade_journal.record(pos, close_price, actual_pnl, 'manual')

    return {'success': True, 'pnl': actual_pnl, 'trades': len(trades)}


@app.get('/api/state')
async def get_state():
    # Build pending orders list from engine order_map + metadata
    pending = []
    for oid, meta in _pending_user_orders.items():
        order = engine.order_map.get(oid)
        if order is None or order.status in ('filled', 'cancelled'):
            continue
        pending.append({
            'order_id': oid,
            'side': order.side,
            'price': order.price,
            'size': order.size,
            'filled': order.filled,
            'leverage': meta.get('leverage', 1),
            'status': order.status,
        })

    return {
        'balance': position_manager.balance,
        'positions': position_manager.get_all(),
        'mark_price': position_manager.mark_price,
        'funding_rate': funding_calc.current_rate,
        'next_funding': funding_calc.next_settlement,
        'insurance_fund': liquidation_monitor.insurance_fund,
        'pending_orders': pending,
    }


@app.post('/api/faucet')
async def claim_faucet():
    return await faucet.claim()


@app.get('/api/news')
async def get_news():
    return {'news': news_system.news_history}
