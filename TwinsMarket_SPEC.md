# TwinsMarket — 完整实现规范

> 本文档面向 Claude Code，包含从架构到每个模块的完整实现细节。按顺序阅读，先理解架构再实现各模块。

---

## 目录

1. [产品定位与核心体验目标](#1-产品定位与核心体验目标)
2. [技术栈选择](#2-技术栈选择)
3. [系统架构：事件总线与时间步长](#3-系统架构事件总线与时间步长)
4. [模块一：合成价格模型（预言机）](#4-模块一合成价格模型预言机)
5. [模块二：撮合引擎](#5-模块二撮合引擎)
6. [模块三：做市商（两个实例）](#6-模块三做市商两个实例)
7. [模块四：噪音交易者](#7-模块四噪音交易者)
8. [模块五：价格锚定者](#8-模块五价格锚定者)
9. [模块六：趋势跟踪者](#9-模块六趋势跟踪者)
10. [模块七：清算监控器](#10-模块七清算监控器)
11. [永续合约核心机制](#11-永续合约核心机制)
12. [用户账户与下单系统](#12-用户账户与下单系统)
13. [市场预设与参数配置](#13-市场预设与参数配置)
14. [虚拟消息系统](#14-虚拟消息系统)
15. [前端界面规范](#15-前端界面规范)
16. [系统启动流程](#16-系统启动流程)
17. [边界情况与异常处理](#17-边界情况与异常处理)
18. [数据持久化](#18-数据持久化)

---

## 1. 产品定位与核心体验目标

TwinsMarket 是一个**零外部依赖的加密货币永续合约模拟交易系统**，在本地单机运行，不需要联网。

**核心体验**：让用户感受到"我的订单真的在影响市场"——下一笔大单，能看到订单簿被吃穿、价格被推动、其他参与者做出反应、市场逐渐消化冲击。

**用户初始资金**：1,000,000 USDT（巨鲸体验）。

**交易品种**：AEN/USDT 永续合约（AEN 是虚构加密货币）。

**价格形成机制**：价格是所有 Agent 在撮合引擎博弈的**涌现结果**，合成价格模型作为隐性引力中心，通过价格锚定者传导，不直接决定市场价格。

---

## 2. 技术栈选择

```
后端：Python 3.11+
  - asyncio 驱动所有 Agent 的并发执行
  - FastAPI 提供 HTTP 下单接口
  - WebSocket（via FastAPI）推送实时数据到前端
  - 无数据库依赖，所有状态在内存中，关键数据序列化到本地 JSON 文件

前端：纯 HTML + CSS + JavaScript（单文件）
  - Canvas 绘制 K 线图、深度图、RSI、VPVR
  - WebSocket 接收实时推送
  - 无框架依赖，无构建工具

运行方式：
  python main.py
  # 自动打开浏览器 http://localhost:8888
```

---

## 3. 系统架构：事件总线与时间步长

### 3.1 事件总线

所有模块之间通过**事件总线**通信，禁止模块之间直接调用对方的方法。

```python
# event_bus.py
import asyncio
from collections import defaultdict
from typing import Callable, Any

class EventBus:
    def __init__(self):
        self._listeners: dict[str, list[Callable]] = defaultdict(list)

    def subscribe(self, event: str, handler: Callable):
        self._listeners[event].append(handler)

    async def publish(self, event: str, data: Any = None):
        for handler in self._listeners[event]:
            if asyncio.iscoroutinefunction(handler):
                await handler(data)
            else:
                handler(data)

bus = EventBus()  # 全局单例
```

**事件清单（完整）**：

| 事件名 | 发布者 | 订阅者 | 数据结构 |
|--------|--------|--------|---------|
| `oracle.tick` | 预言机 | 所有 Agent | `{index_price, volatility, market_state, timestamp}` |
| `oracle.shock` | 预言机 | 做市商、趋势跟踪者 | `{direction, magnitude, timestamp}` |
| `engine.trade` | 撮合引擎 | 所有 Agent、前端推送 | `{price, size, side, aggressor, timestamp}` |
| `engine.orderbook` | 撮合引擎 | 前端推送 | `{asks: [[px,sz],...], bids: [[px,sz],...]}` |
| `engine.kline` | 撮合引擎 | 前端推送 | `{tf, o, h, l, c, v, timestamp}` |
| `engine.liquidation` | 撮合引擎 | 清算监控器、前端推送 | `{user_id, side, price, size}` |
| `user.order` | 用户下单接口 | 撮合引擎 | `{order_id, side, type, price, size, leverage}` |
| `user.impact` | 撮合引擎 | 预言机 | `{net_qty, direction, impact_magnitude}` |
| `market.funding` | 资金费率计算器 | 清算监控器、前端推送 | `{rate, next_settlement, timestamp}` |
| `liquidation.trigger` | 清算监控器 | 撮合引擎 | `{position_id, user_id, side, size, mark_price}` |
| `insurance.depleted` | 清算监控器 | 前端推送 | `{deficit}` |

### 3.2 统一时间步长

所有 Agent 由一个**主循环时钟**驱动，禁止各模块自己创建独立的定时器（防止竞态条件）。

```python
# clock.py
import asyncio
import time

class SimClock:
    """
    主循环：每 TICK_MS 毫秒执行一次。
    各 Agent 注册自己的更新函数和更新频率（每N个tick执行一次）。
    执行顺序固定，见下方优先级表。
    """
    TICK_MS = 50  # 基础时间步长 50ms

    def __init__(self):
        self._agents: list[tuple[int, Callable, str]] = []  # (每N tick, handler, name)
        self._tick = 0

    def register(self, every_n_ticks: int, handler: Callable, name: str):
        self._agents.append((every_n_ticks, handler, name))

    async def run(self):
        while True:
            start = time.monotonic()
            self._tick += 1

            # 按固定优先级顺序执行
            for n, handler, name in self._agents:
                if self._tick % n == 0:
                    try:
                        if asyncio.iscoroutinefunction(handler):
                            await handler()
                        else:
                            handler()
                    except Exception as e:
                        print(f"[Clock] {name} error: {e}")

            elapsed = (time.monotonic() - start) * 1000
            sleep_ms = max(0, self.TICK_MS - elapsed)
            await asyncio.sleep(sleep_ms / 1000)
```

**Agent 执行顺序与频率**（优先级从高到低，每 tick=50ms）：

| 顺序 | Agent | 每N tick执行 | 实际频率 |
|------|-------|-------------|---------|
| 1 | 预言机 | 2 | 100ms |
| 2 | 清算监控器 | 2 | 100ms |
| 3 | 价格锚定者 | 3 | 150ms |
| 4 | 做市商A（激进型） | 4 | 200ms |
| 5 | 做市商B（保守型） | 6 | 300ms |
| 6 | 趋势跟踪者 | 6 | 300ms |
| 7 | 噪音交易者 | 10 | 500ms |
| 8 | 资金费率计算器 | 600 | 30s（模拟时间压缩） |
| 9 | K线聚合器 | 24 | 每1200ms检查一次整点 |
| 10 | 前端推送器 | 1 | 50ms（每tick推送） |

---

## 4. 模块一：合成价格模型（预言机）

### 4.1 定位

预言机扮演"外部现货市场"，输出 `index_price`（指数价格）。它不直接参与撮合，通过价格锚定者把价格信号传导到订单簿。

### 4.2 分层价格模型

```python
# oracle.py
import numpy as np

class Oracle:
    def __init__(self, init_price: float = 23500.0):
        self.index_price = init_price
        self.mu = init_price          # OU过程当前引力中心
        self.theta = init_price       # OU过程长期均衡价格
        self.trend_bias = 0.0         # 动量项
        self.sigma_t = 0.0015         # 当前波动率（GARCH输出）
        self.sigma2 = self.sigma_t**2 # GARCH方差
        self.market_state = 'calm'    # calm / trend / volatile / panic
        self.state_timer = 0          # 当前状态持续的tick数

        # GARCH参数
        self.garch_omega = 1e-6
        self.garch_alpha = 0.10
        self.garch_beta  = 0.85

        # OU参数
        self.kappa = 0.002   # 均值回归速度（每tick）
        self.sigma_ou = 0.0008

        # 动量参数
        self.momentum_alpha = 0.95   # 动量衰减
        self.momentum_beta  = 0.08   # 新收益权重
        self.recent_return  = 0.0

        # 市场状态转移矩阵（每分钟=每24tick评估一次）
        self.transition = {
            'calm':     {'calm':0.970, 'trend':0.020, 'volatile':0.008, 'panic':0.002},
            'trend':    {'calm':0.050, 'trend':0.920, 'volatile':0.025, 'panic':0.005},
            'volatile': {'calm':0.100, 'trend':0.150, 'volatile':0.700, 'panic':0.050},
            'panic':    {'calm':0.200, 'trend':0.100, 'volatile':0.300, 'panic':0.400},
        }
        # 跳跃后临时覆盖转移矩阵
        self.shock_recovery_ticks = 0

        # 各状态的系数
        self.state_vol_mult = {'calm':0.5, 'trend':1.0, 'volatile':2.0, 'panic':4.0}
        self.state_jump_mult = {'calm':0.3, 'trend':0.8, 'volatile':1.5, 'panic':3.0}
        self.state_momentum_mult = {'calm':0.5, 'trend':1.5, 'volatile':0.8, 'panic':0.3}

    async def tick(self):
        dt = 0.1  # 100ms步长对应的时间单位

        # 1. 更新GARCH波动率
        r = self.recent_return
        self.sigma2 = (self.garch_omega
                       + self.garch_alpha * r**2
                       + self.garch_beta * self.sigma2)
        self.sigma2 = max(1e-8, min(self.sigma2, 0.01))  # 钳位
        self.sigma_t = np.sqrt(self.sigma2)

        state_vol = self.sigma_t * self.state_vol_mult[self.market_state]

        # 2. OU过程（基础趋势）
        ou_drift = self.kappa * (self.theta - self.mu)
        ou_noise = self.sigma_ou * state_vol * np.random.randn()
        self.mu += ou_drift + ou_noise

        # 3. 动量项（偏离越大，动量越弱）
        deviation = abs(self.index_price - self.theta) / self.theta
        momentum_decay = max(0.0, 1.0 - deviation * 10)  # 偏离1%时动量归零
        self.trend_bias = (self.momentum_alpha * self.trend_bias
                           + self.momentum_beta * self.recent_return)
        self.trend_bias *= momentum_decay
        momentum_contrib = self.trend_bias * self.state_momentum_mult[self.market_state]

        # 4. 跳跃过程
        jump_contrib = 0.0
        jump_lambda = 0.0005 * self.state_jump_mult[self.market_state]  # 每tick跳跃概率
        if np.random.random() < jump_lambda:
            jump_contrib = self._generate_jump()

        # 5. 微观噪声
        micro_noise = self.index_price * 0.0001 * np.random.randn()

        # 6. 合并
        prev_price = self.index_price
        self.index_price = (self.mu
                            + momentum_contrib * self.index_price
                            + jump_contrib
                            + micro_noise)
        self.index_price = max(self.index_price, 100.0)  # 防止负价格

        # 7. 更新近期收益
        self.recent_return = (self.index_price - prev_price) / prev_price

        # 8. 状态机更新（每24tick约2分钟）
        self.state_timer += 1
        if self.state_timer >= 24:
            self.state_timer = 0
            self._update_state()

        # 9. 广播
        await bus.publish('oracle.tick', {
            'index_price': self.index_price,
            'volatility': self.sigma_t,
            'market_state': self.market_state,
            'timestamp': time.time(),
        })

    def _generate_jump(self) -> float:
        """生成跳跃冲击，不对称（下跌概率略高）"""
        is_down = np.random.random() < 0.55
        is_large = np.random.random() < 0.15

        if is_large:
            magnitude = np.random.uniform(0.02, 0.08)
        else:
            magnitude = np.random.uniform(0.003, 0.01)

        direction = -1 if is_down else 1
        jump_size = direction * magnitude * self.index_price

        # 跳跃后修改状态机恢复路径
        self.shock_recovery_ticks = 24  # 接下来24tick用恢复矩阵
        asyncio.create_task(bus.publish('oracle.shock', {
            'direction': direction,
            'magnitude': magnitude,
            'is_large': is_large,
            'timestamp': time.time(),
        }))

        return jump_size

    def _update_state(self):
        """马尔可夫链状态转移"""
        if self.shock_recovery_ticks > 0:
            # 跳跃恢复期：提高向较低状态转移的概率
            self.shock_recovery_ticks -= 1
            recovery_boost = {'calm':0.05, 'trend':0.05, 'volatile':0.0, 'panic':-0.1}
            trans = dict(self.transition[self.market_state])
            for k, v in recovery_boost.items():
                trans[k] = max(0, trans[k] + v)
            # 重新归一化
            total = sum(trans.values())
            trans = {k: v/total for k, v in trans.items()}
        else:
            trans = self.transition[self.market_state]

        states = list(trans.keys())
        probs = list(trans.values())
        self.market_state = np.random.choice(states, p=probs)

    def apply_user_impact(self, net_qty: float, direction: int):
        """用户大单冲击接入预言机动量"""
        # 冲击基于实际合约量，不含杠杆
        impact = (net_qty * self.index_price) / 1e7  # 归一化
        impact = min(impact, 0.005)  # 单次冲击上限0.5%
        self.trend_bias += direction * impact
```

### 4.3 用户冲击订阅

```python
async def on_user_impact(data):
    oracle.apply_user_impact(data['net_qty'], data['direction'])

bus.subscribe('user.impact', on_user_impact)
```

---

## 5. 模块二：撮合引擎

### 5.1 数据结构

```python
# engine.py
from dataclasses import dataclass, field
from collections import defaultdict
from sortedcontainers import SortedDict
import uuid, time

@dataclass
class Order:
    order_id: str
    owner: str          # agent名称或'user'
    side: str           # 'buy' | 'sell'
    type: str           # 'limit' | 'market' | 'stop'
    price: float        # 限价单价格，市价单为0
    size: float         # 合约数量（AEN）
    filled: float = 0.0
    status: str = 'open'  # open | filled | cancelled | partial
    timestamp: float = field(default_factory=time.time)
    stop_price: float = 0.0   # 止损单触发价

@dataclass
class Trade:
    trade_id: str
    price: float
    size: float
    side: str           # 主动方方向 'buy'|'sell'
    aggressor: str      # 主动方owner
    passive: str        # 被动方owner
    timestamp: float

class MatchingEngine:
    def __init__(self):
        # 订单簿：SortedDict 保证价格有序
        # asks: 价格从低到高，最优卖价在前
        # bids: 价格从高到低，最优买价在前
        self.asks: SortedDict = SortedDict()   # {price: [Order, ...]}
        self.bids: SortedDict = SortedDict(lambda x: -x)  # 负键实现从高到低

        self.stop_orders: list[Order] = []     # 止损单队列
        self.trades: list[Trade] = []          # 成交历史
        self.order_map: dict[str, Order] = {}  # order_id -> Order

        # K线状态
        self.kline_builders: dict[str, KlineBuilder] = {}
        for tf in ['1m', '5m', '15m', '1h', '4h', '1D']:
            self.kline_builders[tf] = KlineBuilder(tf)

        # 市场统计
        self.last_price = 0.0
        self.mid_price = 0.0
        self.ofi_window: list[tuple[float, str]] = []  # (size, side)
```

### 5.2 限价单撮合

```python
    async def submit_order(self, order: Order) -> list[Trade]:
        self.order_map[order.order_id] = order
        trades = []

        if order.type == 'market':
            trades = await self._match_market(order)
        elif order.type == 'limit':
            trades = await self._match_limit(order)
        elif order.type == 'stop':
            self.stop_orders.append(order)

        return trades

    async def _match_market(self, order: Order) -> list[Trade]:
        trades = []
        remaining = order.size

        book = self.asks if order.side == 'buy' else self.bids

        if not book:
            # 流动性真空：无法成交，取消剩余
            order.status = 'cancelled'
            return trades

        for price in list(book.keys()):
            if remaining <= 0:
                break
            level = book[price]
            for passive in list(level):
                if remaining <= 0:
                    break
                # 自成交检测（STP）
                if passive.owner == order.owner:
                    continue
                fill = min(remaining, passive.size - passive.filled)
                trade = await self._execute_fill(order, passive, price, fill)
                trades.append(trade)
                remaining -= fill
                if passive.filled >= passive.size:
                    passive.status = 'filled'
                    level.remove(passive)
            if not level:
                del book[price]

        if remaining > 0:
            order.status = 'partial' if order.filled > 0 else 'cancelled'
        else:
            order.status = 'filled'

        return trades

    async def _match_limit(self, order: Order) -> list[Trade]:
        trades = []

        # 检查是否可以立即成交
        if order.side == 'buy':
            best_ask = next(iter(self.asks), None)
            if best_ask and order.price >= best_ask:
                # 转为市价单逻辑
                return await self._match_market(order)
        else:
            best_bid = next(iter(self.bids), None)
            if best_bid and order.price <= best_bid:
                return await self._match_market(order)

        # 否则挂入订单簿
        self._insert_order(order)
        return trades

    def _insert_order(self, order: Order):
        book = self.bids if order.side == 'buy' else self.asks
        if order.price not in book:
            book[order.price] = []
        book[order.price].append(order)

    async def _execute_fill(self, aggressor: Order, passive: Order,
                             price: float, size: float) -> Trade:
        aggressor.filled += size
        passive.filled += size
        self.last_price = price

        trade = Trade(
            trade_id=str(uuid.uuid4()),
            price=price,
            size=size,
            side=aggressor.side,
            aggressor=aggressor.owner,
            passive=passive.owner,
            timestamp=time.time(),
        )
        self.trades.append(trade)

        # 更新K线
        for builder in self.kline_builders.values():
            builder.update(price, size)

        # 更新OFI窗口
        self.ofi_window.append((size, aggressor.side))
        if len(self.ofi_window) > 200:
            self.ofi_window.pop(0)

        # 更新中间价
        self._update_mid()

        # 发布成交事件
        await bus.publish('engine.trade', {
            'price': price, 'size': size,
            'side': aggressor.side,
            'aggressor': aggressor.owner,
            'timestamp': trade.timestamp,
        })

        # 检查止损单触发
        await self._check_stop_orders(price)

        # 如果主动方是用户，发布冲击事件
        if aggressor.owner == 'user':
            impact = size * price
            direction = 1 if aggressor.side == 'buy' else -1
            await bus.publish('user.impact', {
                'net_qty': size,
                'direction': direction,
                'impact_magnitude': impact,
            })

        return trade

    async def _check_stop_orders(self, last_price: float):
        """止损单触发检测——连锁反应的核心"""
        triggered = []
        remaining_stops = []

        for stop in self.stop_orders:
            should_trigger = False
            if stop.side == 'sell' and last_price <= stop.stop_price:
                should_trigger = True  # 多头止损
            elif stop.side == 'buy' and last_price >= stop.stop_price:
                should_trigger = True  # 空头止损

            if should_trigger:
                triggered.append(stop)
            else:
                remaining_stops.append(stop)

        self.stop_orders = remaining_stops

        # 触发止损单（转为市价单，可能引发连锁）
        for stop in triggered:
            stop.type = 'market'
            await self.submit_order(stop)
```

### 5.3 订单簿快照推送

```python
    def get_snapshot(self, depth: int = 50) -> dict:
        asks = []
        for price in list(self.asks.keys())[:depth]:
            total = sum(o.size - o.filled for o in self.asks[price])
            if total > 0:
                asks.append([price, total])

        bids = []
        for price in list(self.bids.keys())[:depth]:
            total = sum(o.size - o.filled for o in self.bids[price])
            if total > 0:
                bids.append([price, total])

        return {'asks': asks, 'bids': bids, 'mid': self.mid_price}

    def _update_mid(self):
        best_ask = next(iter(self.asks), None)
        best_bid = next(iter(self.bids), None)
        if best_ask and best_bid:
            self.mid_price = (best_ask + best_bid) / 2

    @property
    def ofi(self) -> float:
        """订单流不平衡，范围[0,1]，>0.5表示买压"""
        if not self.ofi_window:
            return 0.5
        buy_vol = sum(s for s, side in self.ofi_window if side == 'buy')
        total = sum(s for s, _ in self.ofi_window)
        return buy_vol / total if total > 0 else 0.5

    @property
    def spread(self) -> float:
        best_ask = next(iter(self.asks), None)
        best_bid = next(iter(self.bids), None)
        if best_ask and best_bid:
            return best_ask - best_bid
        return 0.0
```

### 5.4 K线生成器

```python
class KlineBuilder:
    TF_SECONDS = {'1m':60, '5m':300, '15m':900, '1h':3600, '4h':14400, '1D':86400}

    def __init__(self, tf: str):
        self.tf = tf
        self.seconds = self.TF_SECONDS[tf]
        self.current: dict | None = None
        self.history: list[dict] = []

    def update(self, price: float, volume: float):
        now = time.time()
        bar_start = int(now / self.seconds) * self.seconds  # 整点对齐

        if self.current is None or self.current['t'] != bar_start:
            if self.current:
                self.history.append(dict(self.current))
                if len(self.history) > 1000:
                    self.history.pop(0)
            self.current = {
                't': bar_start, 'tf': self.tf,
                'o': price, 'h': price, 'l': price, 'c': price, 'v': 0.0
            }

        self.current['h'] = max(self.current['h'], price)
        self.current['l'] = min(self.current['l'], price)
        self.current['c'] = price
        self.current['v'] += volume
```

---

## 6. 模块三：做市商（两个实例）

做市商是订单簿深度的核心来源。运行两个实例：
- **做市商A（激进型）**：价差窄，深度浅，补单快，贡献内层流动性
- **做市商B（保守型）**：价差宽，深度厚，补单慢，贡献外层流动性

两者叠加，订单簿形状有层次感，不是均匀矩形。

### 6.1 参数定义

```python
MM_CONFIGS = {
    'mm_aggressive': {
        'base_spread_pct': 0.0004,   # 0.04% 基础价差
        'n_levels': 25,              # 每侧档数
        'level_spacing_inner': 1.0,  # 内层档位间距（USDT）
        'level_spacing_outer': 2.5,  # 外层档位间距（第10档以外）
        'base_size_inner': 0.5,      # 内层基础挂单量（AEN）
        'base_size_outer': 2.0,      # 外层基础挂单量
        'max_inventory': 5000,       # 最大净持仓（AEN）
        'skew_factor': 0.3,          # 持仓偏斜系数
        'repost_delay_ms': (100, 300),  # 补单延迟范围
        'ofi_sensitivity': 0.6,      # OFI敏感度阈值
    },
    'mm_conservative': {
        'base_spread_pct': 0.0012,
        'n_levels': 20,
        'level_spacing_inner': 2.0,
        'level_spacing_outer': 5.0,
        'base_size_inner': 1.5,
        'base_size_outer': 8.0,
        'max_inventory': 8000,
        'skew_factor': 0.2,
        'repost_delay_ms': (300, 700),
        'ofi_sensitivity': 0.7,
    }
}
```

### 6.2 做市商核心逻辑

```python
class MarketMaker:
    def __init__(self, name: str, config: dict, engine: MatchingEngine):
        self.name = name
        self.cfg = config
        self.engine = engine
        self.inventory = 0.0        # 净持仓，正=多，负=空
        self.active_orders: dict[str, Order] = {}  # order_id -> Order
        self.last_index_price = 0.0
        self.current_volatility = 0.001
        self.market_state = 'calm'
        self.is_shocked = False
        self.shock_cooldown = 0     # 剩余冷却tick数
        self.pending_reposts: list[tuple[float, Order]] = []  # (repost_time, order)

        # 订阅事件
        bus.subscribe('oracle.tick', self._on_oracle_tick)
        bus.subscribe('oracle.shock', self._on_shock)
        bus.subscribe('engine.trade', self._on_trade)

    async def _on_oracle_tick(self, data: dict):
        self.last_index_price = data['index_price']
        self.current_volatility = data['volatility']
        self.market_state = data['market_state']

    async def _on_shock(self, data: dict):
        # 立即撤所有单，进入冷却
        await self._cancel_all()
        self.is_shocked = True
        self.shock_cooldown = 6  # 6tick = 300ms冷却

    async def _on_trade(self, data: dict):
        # 记录自己被成交的部分，更新库存
        # 通过检查passive owner更新库存
        pass  # 详见tick()中的库存追踪

    async def tick(self):
        # 处理补单队列
        await self._process_reposts()

        if self.is_shocked:
            self.shock_cooldown -= 1
            if self.shock_cooldown <= 0:
                self.is_shocked = False
                # 恢复报价，价差临时扩大2倍
                await self._post_quotes(spread_multiplier=2.0)
            return

        # 正常运行：检查是否需要重新报价
        mid = self.engine.mid_price
        if mid == 0:
            return

        # 如果持仓超过硬性上限，单侧停止报价
        if abs(self.inventory) >= self.cfg['max_inventory']:
            await self._emergency_hedge()
            return

        await self._post_quotes()

    async def _post_quotes(self, spread_multiplier: float = 1.0):
        """生成并提交两侧报价"""
        await self._cancel_all()

        mid = self.engine.mid_price or self.last_index_price
        cfg = self.cfg

        # 波动率调整价差
        vol_mult = 1.0 + self.current_volatility * 500
        state_mult = {'calm':0.7, 'trend':1.0, 'volatile':1.8, 'panic':3.5}[self.market_state]
        half_spread = (mid * cfg['base_spread_pct'] / 2
                       * vol_mult * state_mult * spread_multiplier)

        # 持仓偏斜
        skew = (self.inventory / cfg['max_inventory']) * cfg['skew_factor'] * mid
        bid_base = mid - half_spread - skew
        ask_base = mid + half_spread - skew

        # OFI调整
        ofi = self.engine.ofi
        ofi_adj = 0.0
        if ofi > cfg['ofi_sensitivity']:
            # 买压大，减少卖单量（避免被大量吃掉）
            ofi_adj = (ofi - cfg['ofi_sensitivity']) * 0.5
        elif ofi < (1 - cfg['ofi_sensitivity']):
            ofi_adj = -(cfg['ofi_sensitivity'] - ofi) * 0.5

        orders = []
        for i in range(cfg['n_levels']):
            # 档位间距：内层小，外层大
            if i < 10:
                spacing = cfg['level_spacing_inner']
                size = cfg['base_size_inner'] * (1 + i * 0.1)
            else:
                spacing = cfg['level_spacing_outer']
                size = cfg['base_size_outer'] * (1 + (i-10) * 0.15)

            # 加入随机扰动（让订单簿不那么规则）
            size_jitter = size * (0.8 + np.random.random() * 0.4)

            bid_price = round(bid_base - i * spacing, 2)
            ask_price = round(ask_base + i * spacing, 2)

            # OFI调整挂单量
            bid_size = size_jitter * (1 - max(0, ofi_adj))
            ask_size = size_jitter * (1 + max(0, ofi_adj))

            if bid_size > 0.01:
                orders.append(Order(
                    order_id=str(uuid.uuid4()), owner=self.name,
                    side='buy', type='limit',
                    price=bid_price, size=round(bid_size, 3)
                ))
            if ask_size > 0.01:
                orders.append(Order(
                    order_id=str(uuid.uuid4()), owner=self.name,
                    side='sell', type='limit',
                    price=ask_price, size=round(ask_size, 3)
                ))

        for order in orders:
            trades = await self.engine.submit_order(order)
            self.active_orders[order.order_id] = order
            # 更新库存
            for t in trades:
                if t.passive == self.name:
                    delta = t.size if t.side == 'sell' else -t.size
                    self.inventory += delta
                    # 调度补单（带延迟）
                    delay_ms = np.random.randint(*self.cfg['repost_delay_ms'])
                    repost_time = time.time() + delay_ms / 1000
                    self.pending_reposts.append((repost_time, order))

    async def _cancel_all(self):
        for oid, order in list(self.active_orders.items()):
            order.status = 'cancelled'
            # 从订单簿中移除
            self.engine._remove_order(order)
        self.active_orders.clear()

    async def _process_reposts(self):
        """处理延迟补单队列"""
        now = time.time()
        remaining = []
        for repost_time, orig_order in self.pending_reposts:
            if now >= repost_time:
                # 补单：以当前mid_price为基础重新计算价格
                # 不使用原价格
                pass  # 实际补单在下次_post_quotes()中统一处理
            else:
                remaining.append((repost_time, orig_order))
        self.pending_reposts = remaining

    async def _emergency_hedge(self):
        """库存超限紧急对冲：只挂有利于减仓的单侧，停止另一侧"""
        await self._cancel_all()
        mid = self.engine.mid_price
        if self.inventory > 0:
            # 持多过多：只挂卖单
            for i in range(self.cfg['n_levels']):
                spacing = self.cfg['level_spacing_inner'] if i < 10 else self.cfg['level_spacing_outer']
                order = Order(
                    order_id=str(uuid.uuid4()), owner=self.name,
                    side='sell', type='limit',
                    price=round(mid + i * spacing, 2),
                    size=round(self.cfg['base_size_inner'] * 1.5, 3)
                )
                await self.engine.submit_order(order)
                self.active_orders[order.order_id] = order
        else:
            # 持空过多：只挂买单
            for i in range(self.cfg['n_levels']):
                spacing = self.cfg['level_spacing_inner'] if i < 10 else self.cfg['level_spacing_outer']
                order = Order(
                    order_id=str(uuid.uuid4()), owner=self.name,
                    side='buy', type='limit',
                    price=round(mid - i * spacing, 2),
                    size=round(self.cfg['base_size_inner'] * 1.5, 3)
                )
                await self.engine.submit_order(order)
                self.active_orders[order.order_id] = order
```

---

## 7. 模块四：噪音交易者

### 7.1 设计原则

噪音交易者模拟一群散户的统计行为，不是单个Agent，而是每tick产生若干随机订单。他们有自己的持仓，会在亏损或超时时平仓。

### 7.2 实现

```python
class NoiseTrader:
    def __init__(self, engine: MatchingEngine):
        self.engine = engine
        self.positions: list[dict] = []  # [{side, entry_price, size, open_time}]
        self.last_index_price = 0.0
        self.current_volatility = 0.001
        self.market_state = 'calm'

        bus.subscribe('oracle.tick', self._on_oracle_tick)

    async def _on_oracle_tick(self, data):
        self.last_index_price = data['index_price']
        self.current_volatility = data['volatility']
        self.market_state = data['market_state']

    async def tick(self):
        mid = self.engine.mid_price
        if mid == 0:
            return

        # 1. 管理现有持仓（止损/超时平仓）
        await self._manage_positions(mid)

        # 2. 下单频率
        base_rate = {'calm':0.5, 'trend':1.0, 'volatile':2.0, 'panic':3.5}[self.market_state]
        vol_boost = 1 + self.current_volatility * 200
        expected_orders = base_rate * vol_boost
        n_orders = np.random.poisson(expected_orders)

        for _ in range(n_orders):
            await self._place_random_order(mid)

    async def _place_random_order(self, mid: float):
        # 方向决策：三力叠加
        ofi = self.engine.ofi
        recent_ret = (mid - self.last_index_price) / self.last_index_price if self.last_index_price else 0

        random_component   = np.random.randn() * 0.4
        momentum_component = np.sign(recent_ret) * min(abs(recent_ret) * 100, 1.0) * 0.35
        herding_component  = (ofi - 0.5) * 2 * 0.25

        signal = random_component + momentum_component + herding_component

        threshold = 0.3
        if signal > threshold:
            side = 'buy'
        elif signal < -threshold:
            side = 'sell'
        else:
            return  # 不下单

        # 订单类型：55%市价，45%限价
        is_market = np.random.random() < 0.55

        # 数量：对数正态分布
        size = round(np.random.lognormal(mean=0.5, sigma=1.2), 3)
        size = max(0.01, min(size, 50.0))  # 钳位

        if is_market:
            order = Order(
                order_id=str(uuid.uuid4()), owner='noise',
                side=side, type='market', price=0.0, size=size
            )
        else:
            # 限价单：在中间价附近随机偏移
            offset = np.random.uniform(0.5, 3.0)
            lim_price = (mid - offset) if side == 'buy' else (mid + offset)
            order = Order(
                order_id=str(uuid.uuid4()), owner='noise',
                side=side, type='limit',
                price=round(lim_price, 2), size=size
            )

        trades = await self.engine.submit_order(order)

        # 记录开仓
        for t in trades:
            if t.aggressor == 'noise':
                self.positions.append({
                    'side': side,
                    'entry_price': t.price,
                    'size': t.size,
                    'open_time': time.time(),
                })

    async def _manage_positions(self, mid: float):
        remaining = []
        for pos in self.positions:
            age = time.time() - pos['open_time']
            pnl_pct = ((mid - pos['entry_price']) / pos['entry_price']
                       * (1 if pos['side'] == 'buy' else -1))

            should_close = False
            if pnl_pct < -0.02:      # 亏损2%止损
                should_close = True
            elif pnl_pct > 0.03:     # 盈利3%止盈
                should_close = True
            elif age > 300:          # 持仓超过300秒超时平仓
                should_close = True
            elif np.random.random() < 0.002:  # 随机平仓概率
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
```

---

## 8. 模块五：价格锚定者

### 8.1 设计说明

原"套利者"和"知情交易者"合并为**价格锚定者**，统一响应 `mid_price` 与 `index_price` 的偏差。偏差小时以限价单为主（被动套利），偏差大时以市价单为主（主动纠偏），同时增加**资金费率套利**行为。

### 8.2 实现

```python
class Arbitrageur:
    def __init__(self, engine: MatchingEngine):
        self.engine = engine
        self.index_price = 0.0
        self.funding_rate = 0.0
        self.inventory = 0.0
        self.max_inventory = 3000.0

        # 套利阈值
        self.entry_threshold = 0.0015    # 0.15% 开始套利
        self.market_order_threshold = 0.003  # 0.3% 改用市价单

        bus.subscribe('oracle.tick', self._on_oracle_tick)
        bus.subscribe('market.funding', self._on_funding)

    async def _on_oracle_tick(self, data):
        self.index_price = data['index_price']

    async def _on_funding(self, data):
        self.funding_rate = data['rate']

    async def tick(self):
        mid = self.engine.mid_price
        if mid == 0 or self.index_price == 0:
            return
        if abs(self.inventory) >= self.max_inventory:
            return

        deviation = (mid - self.index_price) / self.index_price

        # ── 价格偏差套利 ──
        if abs(deviation) > self.entry_threshold:
            aggression = (abs(deviation) - self.entry_threshold) / self.entry_threshold
            base_size = 50 * (1 + aggression * 3)
            base_size = min(base_size, self.max_inventory - abs(self.inventory))

            if abs(deviation) > self.market_order_threshold:
                # 市价单主动纠偏
                side = 'sell' if deviation > 0 else 'buy'
                order = Order(
                    order_id=str(uuid.uuid4()), owner='arbitrageur',
                    side=side, type='market', price=0.0,
                    size=round(base_size, 3)
                )
                trades = await self.engine.submit_order(order)
            else:
                # 限价单被动套利
                side = 'sell' if deviation > 0 else 'buy'
                buffer = 0.0005 * self.index_price  # 小幅让利
                limit_price = (self.index_price + buffer if side == 'sell'
                               else self.index_price - buffer)
                order = Order(
                    order_id=str(uuid.uuid4()), owner='arbitrageur',
                    side=side, type='limit',
                    price=round(limit_price, 2), size=round(base_size, 3)
                )
                await self.engine.submit_order(order)

        # ── 资金费率套利 ──
        # 费率持续偏高（>0.03%）时，做空收费
        funding_threshold = 0.0003
        if abs(self.funding_rate) > funding_threshold:
            funding_side = 'sell' if self.funding_rate > 0 else 'buy'
            funding_size = 30.0
            if abs(self.inventory) + funding_size <= self.max_inventory:
                order = Order(
                    order_id=str(uuid.uuid4()), owner='arbitrageur',
                    side=funding_side, type='limit',
                    price=round(mid * (0.9998 if funding_side == 'buy' else 1.0002), 2),
                    size=funding_size
                )
                await self.engine.submit_order(order)
```

---

## 9. 模块六：趋势跟踪者

### 9.1 设计说明

趋势跟踪者检测价格突破，顺势入场，设置止损。他们是趋势延续的助推力，也是止损踩踏（闪崩）的来源之一。

### 9.2 实现

```python
class TrendFollower:
    def __init__(self, engine: MatchingEngine):
        self.engine = engine
        self.price_history: list[float] = []
        self.positions: list[dict] = []
        self.max_inventory = 2000.0
        self.inventory = 0.0

        bus.subscribe('oracle.tick', self._on_oracle_tick)
        bus.subscribe('oracle.shock', self._on_shock)

    async def _on_oracle_tick(self, data):
        self.price_history.append(data['index_price'])
        if len(self.price_history) > 100:
            self.price_history.pop(0)

    async def _on_shock(self, data):
        # 大跳跃时，趋势跟踪者顺方向入场
        if data['is_large'] and abs(self.inventory) < self.max_inventory:
            side = 'buy' if data['direction'] > 0 else 'sell'
            size = 100.0
            mid = self.engine.mid_price
            stop_price = mid * (0.98 if side == 'buy' else 1.02)  # 2%止损

            order = Order(
                order_id=str(uuid.uuid4()), owner='trend_follower',
                side=side, type='market', price=0.0, size=size
            )
            trades = await self.engine.submit_order(order)
            for t in trades:
                # 注册止损单
                stop = Order(
                    order_id=str(uuid.uuid4()), owner='trend_follower',
                    side='sell' if side == 'buy' else 'buy',
                    type='stop', price=0.0, size=t.size,
                    stop_price=stop_price
                )
                await self.engine.submit_order(stop)

    async def tick(self):
        if len(self.price_history) < 20:
            return

        mid = self.engine.mid_price
        if mid == 0:
            return

        # 检测突破：当前价格超过近20个tick的最高/最低点
        recent_high = max(self.price_history[-20:])
        recent_low  = min(self.price_history[-20:])

        breakout_up   = mid > recent_high * 1.001  # 突破0.1%以上
        breakout_down = mid < recent_low  * 0.999

        if breakout_up and self.inventory < self.max_inventory:
            size = np.random.uniform(20, 80)
            stop_price = mid * 0.985  # 1.5%止损

            order = Order(
                order_id=str(uuid.uuid4()), owner='trend_follower',
                side='buy', type='limit',
                price=round(mid * 1.001, 2), size=round(size, 3)
            )
            trades = await self.engine.submit_order(order)

            for t in trades:
                self.inventory += t.size
                stop = Order(
                    order_id=str(uuid.uuid4()), owner='trend_follower',
                    side='sell', type='stop', price=0.0,
                    size=t.size, stop_price=stop_price
                )
                await self.engine.submit_order(stop)

        elif breakout_down and self.inventory > -self.max_inventory:
            size = np.random.uniform(20, 80)
            stop_price = mid * 1.015

            order = Order(
                order_id=str(uuid.uuid4()), owner='trend_follower',
                side='sell', type='limit',
                price=round(mid * 0.999, 2), size=round(size, 3)
            )
            trades = await self.engine.submit_order(order)

            for t in trades:
                self.inventory -= t.size
                stop = Order(
                    order_id=str(uuid.uuid4()), owner='trend_follower',
                    side='buy', type='stop', price=0.0,
                    size=t.size, stop_price=stop_price
                )
                await self.engine.submit_order(stop)
```

---

## 10. 模块七：清算监控器

### 10.1 设计说明

清算监控器是独立模块，每 100ms 检查所有用户仓位的标记价格是否触及强平线，触发清算流程。

### 10.2 完整清算状态机

```
标记价格触碰强平价
        ↓
生成强平市价单 → 提交撮合引擎
        ↓
计算清算损失 = 强平价值 - 实际成交价值
        ↓
    损失 <= 保证金？
    ├─ 是：用户保证金吸收，仓位清零
    └─ 否（穿仓）：差额 = 损失 - 保证金
            ↓
        保险基金余额 >= 差额？
        ├─ 是：保险基金补偿，仓位清零
        └─ 否：触发 ADL
                ↓
            按盈利比例选取反向仓位强制减仓
            通知被ADL用户
```

### 10.3 实现

```python
class LiquidationMonitor:
    def __init__(self, engine: MatchingEngine, positions: 'PositionManager'):
        self.engine = engine
        self.positions = positions
        self.insurance_fund = 50000.0  # 保险基金初始值（USDT）

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
        # 1. 提交强平市价单
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
        loss = position_value / pos['leverage'] - pos['size'] * abs(avg_fill - pos['entry_price'])

        # 2. 强平手续费
        liq_fee = pos['size'] * avg_fill * 0.005
        total_loss = loss + liq_fee

        # 3. 保证金吸收
        margin = pos['margin']
        if total_loss <= margin:
            # 正常强平
            net_return = margin - total_loss
            await self.positions.close_position(pos['id'], net_return)
        else:
            # 穿仓
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
        """
        自动减仓：从与被强平方向相反、且盈利最多的仓位中按比例减仓
        """
        target_side = 'short' if liquidated_pos['side'] == 'long' else 'long'
        profitable_positions = sorted(
            [p for p in self.positions.get_all()
             if p['side'] == target_side and p['unrealized_pnl'] > 0],
            key=lambda p: p['unrealized_pnl'],
            reverse=True
        )

        remaining_deficit = deficit
        for pos in profitable_positions:
            if remaining_deficit <= 0:
                break
            reduce_size = min(pos['size'], remaining_deficit / self.engine.mid_price)
            close_side = 'sell' if pos['side'] == 'long' else 'buy'
            adl_order = Order(
                order_id=str(uuid.uuid4()),
                owner='adl_engine',
                side=close_side, type='market',
                price=0.0, size=round(reduce_size, 3)
            )
            await self.engine.submit_order(adl_order)
            remaining_deficit -= reduce_size * self.engine.mid_price

    async def _on_funding(self, data):
        # 资金费率结算时从持仓中扣除/增加
        await self.positions.settle_funding(data['rate'])

    async def _on_trade(self, data):
        # 每次成交后，将手续费的一部分注入保险基金
        fee_contribution = data['price'] * data['size'] * 0.0001
        self.insurance_fund += fee_contribution
```

---

## 11. 永续合约核心机制

### 11.1 标记价格（30分钟TWAP防操纵）

```python
class MarkPriceCalculator:
    WINDOW_SECONDS = 1800  # 30分钟

    def __init__(self, engine: MatchingEngine):
        self.engine = engine
        self.index_price = 0.0
        self.mid_history: list[tuple[float, float]] = []  # (timestamp, mid_price)
        self.mark_price = 0.0

        bus.subscribe('oracle.tick', self._on_oracle_tick)

    async def _on_oracle_tick(self, data):
        self.index_price = data['index_price']
        now = time.time()
        mid = self.engine.mid_price
        if mid > 0:
            self.mid_history.append((now, mid))
            # 清理30分钟以外的数据
            cutoff = now - self.WINDOW_SECONDS
            self.mid_history = [(t, p) for t, p in self.mid_history if t > cutoff]

    def compute(self) -> float:
        if not self.mid_history:
            return self.index_price

        # 时间加权平均
        now = time.time()
        weighted_sum = 0.0
        weight_total = 0.0
        for ts, px in self.mid_history:
            w = 1.0 - (now - ts) / self.WINDOW_SECONDS  # 越新权重越大
            weighted_sum += px * w
            weight_total += w

        twap = weighted_sum / weight_total if weight_total > 0 else self.index_price

        # 标记价格 = 指数价格 + 基差移动平均
        basis = twap - self.index_price
        self.mark_price = self.index_price + basis
        return self.mark_price
```

### 11.2 仓位管理器

```python
class PositionManager:
    MAINTENANCE_MARGIN_RATE = 0.01   # 1% 维持保证金率
    MAKER_FEE = 0.0002               # 0.02%
    TAKER_FEE = 0.0005               # 0.05%
    LIQ_FEE   = 0.005                # 0.50%
    FUNDING_INTERVAL = 28800         # 8小时（秒），模拟时压缩为3600

    def __init__(self):
        self.positions: dict[str, dict] = {}
        self.balance = 1_000_000.0   # 用户初始余额
        self.mark_price_calc = None  # 注入
        self.next_funding_time = time.time() + self.FUNDING_INTERVAL

    @property
    def mark_price(self) -> float:
        return self.mark_price_calc.compute() if self.mark_price_calc else 0.0

    def liquidation_price(self, pos: dict) -> float:
        if pos['side'] == 'long':
            return pos['entry_price'] * (1 - 1/pos['leverage'] + self.MAINTENANCE_MARGIN_RATE)
        else:
            return pos['entry_price'] * (1 + 1/pos['leverage'] - self.MAINTENANCE_MARGIN_RATE)

    def breakeven_price(self, pos: dict) -> float:
        """含手续费的损益两平价"""
        fee_rate = self.TAKER_FEE * 2  # 开仓+平仓
        if pos['side'] == 'long':
            return pos['entry_price'] * (1 + fee_rate)
        else:
            return pos['entry_price'] * (1 - fee_rate)

    def unrealized_pnl(self, pos: dict) -> float:
        mark = self.mark_price
        if pos['side'] == 'long':
            return (mark - pos['entry_price']) * pos['size']
        else:
            return (pos['entry_price'] - mark) * pos['size']

    def adl_risk(self, pos: dict) -> int:
        """ADL风险等级 1-5"""
        pnl = self.unrealized_pnl(pos)
        margin = pos['margin']
        if pnl <= 0:
            return 1
        ratio = pnl / margin
        if ratio < 0.2: return 1
        if ratio < 0.5: return 2
        if ratio < 1.0: return 3
        if ratio < 2.0: return 4
        return 5

    async def open_position(self, side: str, size: float, leverage: int,
                             entry_price: float, order_type: str) -> dict | None:
        fee_rate = self.TAKER_FEE if order_type == 'market' else self.MAKER_FEE
        notional = size * entry_price
        margin_required = notional / leverage
        fee = notional * fee_rate

        if margin_required + fee > self.balance:
            return None  # 余额不足

        self.balance -= (margin_required + fee)

        pos = {
            'id': str(uuid.uuid4()),
            'user_id': 'user',
            'side': side,
            'size': size,
            'entry_price': entry_price,
            'leverage': leverage,
            'margin': margin_required,
            'unrealized_pnl': 0.0,
        }
        self.positions[pos['id']] = pos
        return pos

    async def close_position(self, pos_id: str, returned_margin: float):
        pos = self.positions.pop(pos_id, None)
        if pos:
            self.balance += returned_margin

    async def settle_funding(self, rate: float):
        """资金费率结算"""
        mark = self.mark_price
        for pos in self.positions.values():
            notional = pos['size'] * mark
            funding_payment = notional * rate
            if pos['side'] == 'long':
                # 正费率：多头付给空头
                pos['margin'] -= funding_payment
            else:
                pos['margin'] += funding_payment

            # 保证金不足时触发强平
            if pos['margin'] <= 0:
                await bus.publish('liquidation.trigger', pos)

    def get_all(self) -> list[dict]:
        mark = self.mark_price
        for pos in self.positions.values():
            pos['unrealized_pnl'] = self.unrealized_pnl(pos)
        return list(self.positions.values())
```

### 11.3 资金费率计算器

```python
class FundingRateCalculator:
    MAX_RATE = 0.0075   # 单次最大资金费率 ±0.75%（硬性钳位）
    BASE_RATE = 0.0001  # 基础利率 0.01%

    def __init__(self, mark_calc: MarkPriceCalculator):
        self.mark_calc = mark_calc
        self.current_rate = 0.0
        self.next_settlement = time.time() + 3600  # 模拟时间压缩为1小时

    async def tick(self):
        index = self.mark_calc.index_price
        mark  = self.mark_calc.mark_price
        if index == 0:
            return

        premium_index = (mark - index) / index
        clamp = lambda x, lo, hi: max(lo, min(hi, x))
        rate = premium_index + clamp(self.BASE_RATE - premium_index, -0.0005, 0.0005)

        # 硬性上下限
        self.current_rate = clamp(rate, -self.MAX_RATE, self.MAX_RATE)

        now = time.time()
        if now >= self.next_settlement:
            self.next_settlement = now + 3600
            await bus.publish('market.funding', {
                'rate': self.current_rate,
                'next_settlement': self.next_settlement,
                'timestamp': now,
            })
```

---

## 12. 用户账户与下单系统

### 12.1 HTTP 下单接口（FastAPI）

```python
# api.py
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class OrderRequest(BaseModel):
    side: str           # 'buy' | 'sell'
    type: str           # 'market' | 'limit'
    price: float = 0.0
    size: float
    leverage: int = 1
    margin_mode: str = 'isolated'  # 'isolated' | 'cross'

@app.post('/api/order')
async def place_order(req: OrderRequest):
    # 1. 参数校验
    if req.leverage < 1 or req.leverage > 100:
        return {'error': 'leverage must be 1-100'}
    if req.size <= 0:
        return {'error': 'size must be positive'}

    # 2. 计算成交价格
    mid = engine.mid_price
    entry_price = mid if req.type == 'market' else req.price

    # 3. 开仓
    pos = await position_manager.open_position(
        side='long' if req.side == 'buy' else 'short',
        size=req.size,
        leverage=req.leverage,
        entry_price=entry_price,
        order_type=req.type,
    )
    if not pos:
        return {'error': 'insufficient balance'}

    # 4. 提交到撮合引擎
    order = Order(
        order_id=str(uuid.uuid4()), owner='user',
        side=req.side, type=req.type,
        price=req.price, size=req.size,
    )
    trades = await engine.submit_order(order)

    return {'success': True, 'position': pos, 'trades': len(trades)}

@app.post('/api/close')
async def close_position(position_id: str, order_type: str = 'market'):
    pos = position_manager.positions.get(position_id)
    if not pos:
        return {'error': 'position not found'}

    close_side = 'sell' if pos['side'] == 'long' else 'buy'
    order = Order(
        order_id=str(uuid.uuid4()), owner='user',
        side=close_side, type=order_type,
        price=0.0, size=pos['size'],
    )
    trades = await engine.submit_order(order)
    pnl = position_manager.unrealized_pnl(pos)
    await position_manager.close_position(position_id, pos['margin'] + pnl)

    return {'success': True, 'pnl': pnl}

@app.get('/api/state')
async def get_state():
    return {
        'balance': position_manager.balance,
        'positions': position_manager.get_all(),
        'mark_price': position_manager.mark_price,
        'funding_rate': funding_calc.current_rate,
        'next_funding': funding_calc.next_settlement,
        'insurance_fund': liquidation_monitor.insurance_fund,
    }
```

### 12.2 水龙头系统

```python
class Faucet:
    REPLENISH_AMOUNT = 500_000.0  # 每次补充50万USDT
    TRIGGER_THRESHOLD = 100_000.0  # 余额低于10万时提示
    COOLDOWN_SECONDS = 600        # 10分钟冷却

    def __init__(self, position_manager: PositionManager):
        self.pm = position_manager
        self.last_used = 0.0
        self.cooldown_remaining = 0

    def should_show(self) -> bool:
        return self.pm.balance < self.TRIGGER_THRESHOLD

    async def claim(self) -> dict:
        now = time.time()
        if now - self.last_used < self.COOLDOWN_SECONDS:
            remaining = int(self.COOLDOWN_SECONDS - (now - self.last_used))
            return {'success': False, 'cooldown_remaining': remaining}

        self.pm.balance += self.REPLENISH_AMOUNT
        self.last_used = now
        return {'success': True, 'amount': self.REPLENISH_AMOUNT,
                'new_balance': self.pm.balance}

@app.post('/api/faucet')
async def claim_faucet():
    return await faucet.claim()
```

---

## 13. 市场预设与参数配置

用户可在启动前（或通过设置界面）选择预设，也可自定义所有参数。

### 13.1 三套预设

```python
PRESETS = {
    'retail': {
        'name': '散户市场',
        'description': '流动性差的山寨币，大单容易推价',
        'mm_aggressive': {**MM_CONFIGS['mm_aggressive'],
                          'base_spread_pct': 0.003, 'n_levels': 15},
        'mm_conservative': {**MM_CONFIGS['mm_conservative'],
                            'base_spread_pct': 0.008, 'n_levels': 10},
        'noise_base_rate': 0.8,
        'arbitrageur_entry_threshold': 0.005,
        'oracle_volatility_mult': 1.5,
    },
    'mainstream': {
        'name': '主流市场（默认）',
        'description': '接近BTC/ETH的感觉，巨鲸冲击可见但会被吸收',
        'mm_aggressive': MM_CONFIGS['mm_aggressive'],
        'mm_conservative': MM_CONFIGS['mm_conservative'],
        'noise_base_rate': 1.0,
        'arbitrageur_entry_threshold': 0.0015,
        'oracle_volatility_mult': 1.0,
    },
    'extreme': {
        'name': '极端行情',
        'description': '体验市场崩盘或暴涨，强平潮，流动性危机',
        'mm_aggressive': {**MM_CONFIGS['mm_aggressive'],
                          'base_spread_pct': 0.008, 'n_levels': 12,
                          'repost_delay_ms': (500, 1500)},
        'mm_conservative': {**MM_CONFIGS['mm_conservative'],
                            'base_spread_pct': 0.02, 'n_levels': 8},
        'noise_base_rate': 2.5,
        'arbitrageur_entry_threshold': 0.005,
        'oracle_volatility_mult': 4.0,
        'oracle_initial_state': 'panic',
    },
}
```

---

## 14. 虚拟消息系统

### 14.1 消息模板

消息分类：宏观、链上数据、监管动态、KOL动态、机构动态、技术面。每条消息有情绪标签（利多/利空/中性）但内容故意模糊，让用户自己判断。

### 14.2 实现

```python
import random

NEWS_TEMPLATES = [
    # 格式: (tag, source, template, trigger_condition)
    # trigger_condition: None=随机, 'bull_run'=价格大涨时, 'bear'=价格大跌时, 'high_vol'=高波动时

    ('bull', '链上数据',
     'Whale Alert：大额钱包向交易所转入 {amount} AEN，可能为做市或套利，方向尚不明确。',
     None),

    ('bear', '链上数据',
     '链上数据显示过去1小时交易所净流入 {amount} AEN，历史上该信号与短期价格承压相关，但并非绝对。',
     None),

    ('bull', '机构动态',
     '知名做市商 {fund_name} 据悉扩大了 AEN 现货敞口，目标配置约 {amount} USDT，消息来源未获证实。',
     None),

    ('bear', '机构动态',
     '对冲基金 {fund_name} 在最新报告中将 AEN 评级下调，理由是估值已充分反映短期利好。',
     None),

    ('neut', '技术面',
     'AEN 当前价格正在测试 {price} 一线，该位置历史上曾多次形成分歧，多空双方均有较大挂单。',
     None),

    ('bull', '技术面',
     '日线 MACD 金叉形成，RSI 从超卖区域回升，历史上该组合信号后续胜率约 {pct}%，样本量有限。',
     None),

    ('bear', 'KOL动态',
     '知名分析师 @{kol_name} 发推："AEN 上涨缺乏现货量支撑，资金费率连续偏正，警惕多头踩踏。" 该分析师历史胜率约 55%。',
     None),

    ('bull', '宏观',
     '美联储官员发表讲话称通胀数据"令人鼓舞"，市场降息预期升温，风险资产普遍走强。',
     None),

    ('bear', '监管动态',
     '{country} 监管机构据报正在研究对加密衍生品征收额外资本利得税，业内人士认为影响程度取决于税率区间。',
     None),

    ('bull', '链上数据',
     'AEN 链上活跃地址数近7日增长 {pct}%，链上活动回暖，但与价格的相关性存在滞后。',
     None),

    ('bear', '技术面',
     'AEN 当前价格下方 {price} 存在大量多头止损单，若跌破可能引发连锁清算，需关注。',
     'high_vol'),

    ('bull', '机构动态',
     '{fund_name} 季报显示其 AEN 持仓占总组合 {pct}%，较上季度提升，但持仓成本约 {price}。',
     None),

    ('neut', '宏观',
     '全球加密货币总市值维持在 {amount} 万亿美元附近，AEN 市占率近期基本稳定。',
     None),

    ('bear', 'KOL动态',
     '链上分析师 @{kol_name}：大户地址过去24小时持续减仓，需警惕流动性陷阱。',
     'bear'),

    ('bull', '链上数据',
     '交易所 AEN 储备量连续3日净流出，历史上该信号有时先于价格上涨，但也可能是持币者转移到冷钱包。',
     'bull_run'),
]

FILL_VALUES = {
    'amount': ['12,400', '45,000', '8,800', '23,100', '5.2亿', '1.8亿'],
    'fund_name': ['Apex Capital', 'GreyScale Macro', 'Citadel Digital',
                  'Jump Trading', 'Wintermute', 'DWF Labs'],
    'kol_name': ['MarketSage', 'CryptoOracle', 'WhaleWatcher', 'AlphaSeeker'],
    'country': ['美国某州', '欧盟', '英国', '韩国', '香港'],
    'pct': ['52', '58', '61', '47', '55', '63'],
    'price': lambda: f"{random.randint(22000, 25000):,}",
}

class NewsSystem:
    def __init__(self):
        self.news_history: list[dict] = []
        self.last_news_time = 0.0
        self.min_interval = 45   # 最少45秒一条
        self.max_interval = 180  # 最多3分钟一条

    async def tick(self, market_state: str, price_change_pct: float):
        now = time.time()
        if now - self.last_news_time < self.min_interval:
            return

        # 高波动/大涨大跌时更容易触发新闻
        base_prob = 0.02  # 每tick约2%概率
        if market_state in ('volatile', 'panic'):
            base_prob = 0.05
        if abs(price_change_pct) > 0.02:
            base_prob = 0.08

        if random.random() > base_prob:
            return

        self.last_news_time = now

        # 根据市场状态选择偏向性模板
        if price_change_pct > 0.01:
            condition = 'bull_run'
        elif price_change_pct < -0.01:
            condition = 'bear'
        elif market_state in ('volatile', 'panic'):
            condition = 'high_vol'
        else:
            condition = None

        candidates = [t for t in NEWS_TEMPLATES
                      if t[3] is None or t[3] == condition]
        tag, source, template, _ = random.choice(candidates)

        # 填充模板
        text = template
        for key, values in FILL_VALUES.items():
            if '{' + key + '}' in text:
                val = values() if callable(values) else random.choice(values)
                text = text.replace('{' + key + '}', val)

        news = {
            'id': str(uuid.uuid4()),
            'tag': tag,
            'source': source,
            'body': text,
            'timestamp': now,
            'time_str': time.strftime('%H:%M', time.localtime(now)),
        }
        self.news_history.insert(0, news)
        if len(self.news_history) > 50:
            self.news_history.pop()
```

---

## 15. 前端界面规范

前端是单个 HTML 文件（`frontend/index.html`），通过 WebSocket 接收实时数据。

### 15.1 WebSocket 推送协议

服务端每 50ms 推送一次完整状态包：

```json
{
  "type": "tick",
  "price": 23512.63,
  "mark_price": 23498.20,
  "index_price": 23475.80,
  "orderbook": {
    "asks": [[23513.78, 1.329], [23514.93, 4.295], ...],
    "bids": [[23511.48, 1.724], [23510.33, 2.582], ...]
  },
  "trades": [
    {"price": 23512.63, "size": 0.234, "side": "buy", "timestamp": 1234567890}
  ],
  "klines": {
    "15m": {"o":23480,"h":23550,"l":23460,"c":23512,"v":1234.5,"t":1234567800}
  },
  "funding_rate": 0.000123,
  "next_funding": 1234567890,
  "positions": [...],
  "balance": 985432.10,
  "mark_price": 23498.20,
  "insurance_fund": 52341.20,
  "market_state": "calm",
  "news": [...],
  "faucet": {"should_show": false, "cooldown": 0}
}
```

### 15.2 图表规范

**K线图**：
- Canvas 绘制，每帧重绘
- 纵轴价格范围：当前视窗内 K 线最高价 +3% 至最低价 -3%
- VPVR 与价格纵轴严格对齐：每个价格 bin 的 Y 坐标用 `toY()` 函数计算，与 K 线使用相同的坐标系
- 标注线（开仓价/强平价/保本价/标记价）使用同一 `toY()` 函数定位

**订单簿**：
- 档数 = `Math.floor(容器高度 / 17)`（行高固定17px）
- 服务端返回足够深度（50档），前端截取需要的档数显示
- 背景条宽度用 CSS `transition: width 0.18s cubic-bezier(0.25,0.46,0.45,0.94)` 平滑过渡
- 背景条宽度 = `(累计量 / 最大累计量) * 100%`
- 数据更新时：直接修改 DOM 元素的 `style.width`，不重建 DOM

**深度图**：
- 阶梯形（非平滑曲线），每个价格档位是一个水平台阶
- X 轴：从最低bid价到最高ask价，撑满整个容器宽度
- Y 轴：从0到最大累计量
- 每帧对深度数据做指数平滑：`smooth[i] = smooth[i] + 0.18 * (target[i] - smooth[i])`
- Canvas 每帧重绘（requestAnimationFrame）

### 15.3 指标

内置指标列表（可通过指标选择器开关）：

| 指标 | 位置 | 默认开启 |
|------|------|---------|
| EMA(20) | 主图叠加 | 是 |
| VPVR | 主图右侧 | 是 |
| RSI(14) | 子图 | 是 |
| MACD | 子图 | 否 |
| 布林带 | 主图叠加 | 否 |
| VWAP | 主图叠加 | 否 |
| SMA(50) | 主图叠加 | 否 |
| ATR(14) | 子图 | 否 |
| 随机RSI | 子图 | 否 |
| OFI | 子图（独家） | 否 |
| 资金费率历史 | 子图（独家） | 否 |

### 15.4 系统日志（Agent反应可见性）

底部日志面板实时显示 Agent 行为，帮助用户理解市场结构：

```
[10:42:31] 🐋 用户大单：买入 10,000 AEN，价格冲击 +0.34%
[10:42:32] ⚖️  价格锚定者：检测到偏差 +0.34%，开始反向建仓（卖出市价单）
[10:42:33] 📊 做市商A：价差扩大至 0.12%，重新报价中
[10:42:35] 🔁 趋势跟踪者：检测到上破阻力，顺势开多 45 AEN
[10:42:38] ⚖️  价格锚定者：偏差收窄至 +0.08%，切换为限价单模式
[10:42:45] 💧 资金费率：+0.0145%，下次结算 03:17:15 后
[10:43:01] ⚠️  市场状态：平静 → 趋势
```

---

## 16. 系统启动流程

### 16.1 冷启动预热

系统启动时需要预热，不能直接进入稳态：

```python
async def cold_start():
    """
    预热流程：模拟过去2小时的市场历史，初始化K线和VPVR数据
    预热期间前端显示加载动画，不接受用户下单
    """
    print("[Startup] 初始化市场历史...")

    # 1. 生成过去120分钟的合成价格路径
    init_price = 23500.0
    synthetic_prices = oracle.generate_historical_path(
        n_steps=7200,  # 120分钟 × 60秒 × 1步/秒
        start_price=init_price
    )

    # 2. 用历史价格初始化K线
    for i, px in enumerate(synthetic_prices):
        ts = time.time() - (7200 - i)
        for builder in engine.kline_builders.values():
            builder.update_historical(px, volume=random.uniform(0.5, 5.0), ts=ts)

    # 3. 用历史成交初始化VPVR数据
    # VPVR数据存储在 vpvr_history 中，前端从这里读取

    # 4. 启动做市商（价差从2倍逐渐收窄）
    mm_aggressive.startup_spread_mult = 2.0
    mm_conservative.startup_spread_mult = 2.5

    print("[Startup] 完成，开始正常运行")
```

### 16.2 主启动文件

```python
# main.py
import asyncio
import uvicorn
import webbrowser

async def main():
    # 初始化所有模块
    global engine, oracle, position_manager, liquidation_monitor
    global mm_aggressive, mm_conservative, noise_trader
    global arbitrageur, trend_follower, funding_calc, news_system, faucet

    engine             = MatchingEngine()
    oracle             = Oracle(init_price=23500.0)
    position_manager   = PositionManager()
    mark_calc          = MarkPriceCalculator(engine)
    position_manager.mark_price_calc = mark_calc
    liquidation_monitor = LiquidationMonitor(engine, position_manager)
    mm_aggressive      = MarketMaker('mm_aggressive', MM_CONFIGS['mm_aggressive'], engine)
    mm_conservative    = MarketMaker('mm_conservative', MM_CONFIGS['mm_conservative'], engine)
    noise_trader       = NoiseTrader(engine)
    arbitrageur        = Arbitrageur(engine)
    trend_follower     = TrendFollower(engine)
    funding_calc       = FundingRateCalculator(mark_calc)
    news_system        = NewsSystem()
    faucet             = Faucet(position_manager)

    # 注册到时钟
    clock = SimClock()
    clock.register(2,  oracle.tick,                 'Oracle')
    clock.register(2,  liquidation_monitor.tick,    'Liquidation')
    clock.register(3,  arbitrageur.tick,            'Arbitrageur')
    clock.register(4,  mm_aggressive.tick,          'MM_Aggressive')
    clock.register(6,  mm_conservative.tick,        'MM_Conservative')
    clock.register(6,  trend_follower.tick,         'TrendFollower')
    clock.register(10, noise_trader.tick,           'NoiseTrader')
    clock.register(600, funding_calc.tick,          'FundingRate')
    clock.register(1,  push_to_frontend,            'FrontendPush')

    # 预热
    await cold_start()

    # 启动 FastAPI（后台任务）
    config = uvicorn.Config(app, host='127.0.0.1', port=8888, log_level='warning')
    server = uvicorn.Server(config)
    asyncio.create_task(server.serve())

    # 打开浏览器
    await asyncio.sleep(1)
    webbrowser.open('http://localhost:8888')

    # 启动主时钟
    await clock.run()

if __name__ == '__main__':
    asyncio.run(main())
```

---

## 17. 边界情况与异常处理

| 情况 | 处理方式 |
|------|---------|
| 市价单吃穿全部订单簿 | 剩余量取消，WebSocket推送 `{type:'order_failed', reason:'insufficient_liquidity'}` |
| 流动性真空（做市商撤单后订单簿为空） | 市价单暂时挂起，等待最多2秒，超时取消 |
| 自成交（STP） | 撮合引擎跳过同owner的单，不成交 |
| 用户余额不足 | 开仓拒绝，返回错误，触发水龙头提示 |
| 保险基金耗尽 | 触发ADL，前端显示红色警告横幅 |
| 价格超出合理范围（<100 或 >1,000,000） | 预言机内部钳位，不向外广播异常值 |
| WebSocket断线重连 | 前端检测到断线后每2秒重试，重连成功后请求完整状态快照 |
| Python异常崩溃 | Clock捕获异常并打印，不中断主循环，该tick跳过出错模块 |

---

## 18. 数据持久化

```python
# persistence.py
import json, os

SAVE_PATH = './data/state.json'

async def save_state():
    """每60秒保存一次关键状态"""
    state = {
        'balance': position_manager.balance,
        'positions': position_manager.get_all(),
        'insurance_fund': liquidation_monitor.insurance_fund,
        'klines': {
            tf: {
                'history': builder.history[-200:],
                'current': builder.current,
            }
            for tf, builder in engine.kline_builders.items()
        },
        'journal': trade_journal.entries[-500:],  # 仓位记录
        'timestamp': time.time(),
    }
    os.makedirs('./data', exist_ok=True)
    with open(SAVE_PATH, 'w') as f:
        json.dump(state, f)

async def load_state():
    """启动时尝试加载上次状态"""
    if not os.path.exists(SAVE_PATH):
        return False
    try:
        with open(SAVE_PATH) as f:
            state = json.load(f)
        position_manager.balance = state['balance']
        liquidation_monitor.insurance_fund = state['insurance_fund']
        # 恢复K线历史...
        print(f"[Persistence] 已恢复上次状态（{time.ctime(state['timestamp'])}）")
        return True
    except Exception as e:
        print(f"[Persistence] 加载失败，使用默认状态: {e}")
        return False
```

### 18.1 仓位记录（交易日志）

```python
class TradeJournal:
    def __init__(self):
        self.entries: list[dict] = []

    def record(self, pos: dict, close_price: float, pnl: float, close_reason: str):
        entry = {
            'id': str(uuid.uuid4()),
            'open_time': pos.get('open_time', 0),
            'close_time': time.time(),
            'side': pos['side'],
            'leverage': pos['leverage'],
            'size': pos['size'],
            'entry_price': pos['entry_price'],
            'close_price': close_price,
            'pnl': pnl,
            'pnl_pct': pnl / pos['margin'] * 100,
            'close_reason': close_reason,  # 'manual' | 'liquidation' | 'stop_loss' | 'take_profit'
            'margin_used': pos['margin'],
        }
        self.entries.insert(0, entry)
        if len(self.entries) > 1000:
            self.entries.pop()
```

---

## 附录：目录结构

```
TwinsMarket/
├── main.py                 # 入口
├── clock.py                # 主时钟
├── event_bus.py            # 事件总线
├── engine.py               # 撮合引擎
├── oracle.py               # 合成价格模型
├── agents/
│   ├── market_maker.py     # 做市商
│   ├── noise_trader.py     # 噪音交易者
│   ├── arbitrageur.py      # 价格锚定者
│   └── trend_follower.py   # 趋势跟踪者
├── contracts/
│   ├── positions.py        # 仓位管理器
│   ├── liquidation.py      # 清算监控器
│   ├── funding.py          # 资金费率计算器
│   └── mark_price.py       # 标记价格
├── api.py                  # FastAPI 接口
├── ws_push.py              # WebSocket 推送
├── news.py                 # 虚拟消息系统
├── persistence.py          # 数据持久化
├── faucet.py               # 水龙头
├── presets.py              # 市场预设
├── frontend/
│   └── index.html          # 前端单文件
└── data/
    └── state.json          # 持久化状态
```

---

*文档结束。本文档包含 TwinsMarket 所有模块的完整实现规范，Claude Code 可按模块顺序依次实现，各模块通过事件总线解耦，可独立测试。*
