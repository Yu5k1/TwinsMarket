# TwinsMarket — 价格逻辑全梳理（v2，2026-05 重写）

本文档同步当前代码库的价格生成、撮合、报价、套利、标记价与持久化全链路，包含所有已生效的安全阀与冷启动锚定修复。读完后你应该能回答：

- 一笔价格是怎么从 Oracle 数学模型 → 订单簿 → K 线 → 前端的
- 哪些地方有反馈正反馈风险，是怎么截断的
- 重启 / 冷启动时锚点是怎么对齐的（防止 16k→36k 那种飞涨）
- 市价单为什么不会再被静默丢弃

## 目录
1. [整体架构](#1-整体架构)
2. [时钟与刷新率](#2-时钟与刷新率)
3. [三套独立价格](#3-三套独立价格)
4. [冷启动与状态恢复](#4-冷启动与状态恢复)
5. [Oracle 价格模型](#5-oracle-价格模型)
6. [市场状态机](#6-市场状态机)
7. [订单簿与撮合引擎](#7-订单簿与撮合引擎)
8. [做市商报价逻辑](#8-做市商报价逻辑)
9. [套利者](#9-套利者)
10. [趋势跟随者与噪声交易者](#10-趋势跟随者与噪声交易者)
11. [中间价与标记价](#11-中间价与标记价)
12. [用户大单冲击](#12-用户大单冲击)
13. [资金费率与清算](#13-资金费率与清算)
14. [K 线构建](#14-k-线构建)
15. [持久化](#15-持久化)
16. [安全阀汇总（防飞涨）](#16-安全阀汇总防飞涨)
17. [三种预设市场](#17-三种预设市场)

---

## 1. 整体架构

```
                     Oracle（指数价，纯数学模型）
                          │ oracle.tick 每 100ms 广播
                          ▼
   ┌──────────┬─────────────────────┬──────────────────┐
   │          │                     │                  │
做市商      套利者              趋势跟随者          噪声交易者
（×2）      （锚定 mid↔index）   （突破追势）      （随机背景流）
   │          │                     │                  │
   └──────────┴──────┬──────────────┴──────────────────┘
                     ▼
              MatchingEngine 订单簿
              ├─ asks / bids（SortedDict）
              ├─ 限价 + 市价 + 止损
              └─ engine.trade 广播
                     │
        ┌────────────┼────────────┐
        ▼            ▼            ▼
   KlineBuilder   mid_price    MarkPriceCalculator
   （×6 TF）     （order-book 中点）  （index + clamped TWAP basis）
                     │
                     ▼
              WSPushManager
              50ms 全量快照 → 前端
```

文件对应：
- 价格模型：[oracle.py](oracle.py)
- 撮合：[engine.py](engine.py)
- 做市商：[agents/market_maker.py](agents/market_maker.py)
- 套利：[agents/arbitrageur.py](agents/arbitrageur.py)
- 趋势：[agents/trend_follower.py](agents/trend_follower.py)
- 噪声：[agents/noise_trader.py](agents/noise_trader.py)
- 标记价：[contracts/mark_price.py](contracts/mark_price.py)
- 持久化：[persistence.py](persistence.py)
- 启动 / 锚定：[main.py](main.py)

---

## 2. 时钟与刷新率

底层时钟 [`SimClock`](clock.py) 每 **50 ms** 一个 tick。所有 agent 注册一个 "每 N tick 触发一次" 的 handler。

| 组件 | 每 N ticks | 实际间隔 | 说明 |
|------|-----------|---------|------|
| `FrontendPush` | 1 | **50 ms** | WebSocket 推全量快照 |
| `Oracle` | 2 | **100 ms** | 更新指数价、波动率、状态 |
| `LiquidationMonitor` | 2 | **100 ms** | 扫描清算线 |
| `Arbitrageur` | **2** | **100 ms** | 套利偏差检查（从 150 ms 提频） |
| `MM_Aggressive` | **2** | **100 ms** | 激进做市商刷新报价（从 200 ms 提频）|
| `MM_Conservative` | **3** | **150 ms** | 保守做市商刷新报价（从 300 ms 提频）|
| `TrendFollower` | **3** | **150 ms** | 突破追势（从 300 ms 提频）|
| `NoiseTrader` | **4** | **200 ms** | 随机噪声订单（从 500 ms 提频）|
| `NewsSystem` | 24 | 1200 ms | 资讯生成 |
| `FundingRate` | 600 | 30 s | 计算资金费率 |
| `Persistence` | 1200 | 60 s | 写 `state.json` |

> 提频之后 1 分钟内 ≥ 1000 笔有效成交（之前 ~50 笔），K 线相邻 bar 的 open/prev_close 跳价 < 0.06%，影线绝对长度（高低差）回到 50–130 USDT 的正常 BTC 1m 形态。

---

## 3. 三套独立价格

| 名称 | 来源 | 用途 | 速度 |
|------|------|------|------|
| `index_price` | Oracle 数学模型 | 资金费率分子、套利锚 | 每 100 ms 更新 |
| `mid_price` | `(best_bid + best_ask) / 2` | 做市商报价中心、市价单参考 | 每次限价插入/成交时更新 |
| `mark_price` | `index + clamp(twap_basis, ±0.5%)` | 计算未实现盈亏、清算触发 | 计算时取值（属性式） |

三者不强同步：套利者负责把 `mid` 拉向 `index`；TWAP 把 `mid` 的瞬时偏差平滑后叠加到 `mark` 上。

---

## 4. 冷启动与状态恢复

入口：[`main.cold_start()`](main.py)。两条互斥路径：

### 4.1 跳过路径（saved klines < 1 小时新）

```python
# 1. 选最近一根「合理」的 close 当锚（剔除 ≤100、≥1e6 这种污染值）
def _sane(p): return p is not None and 100 < float(p) < 1_000_000
last_close = current.c if _sane(current.c) else history[-1].c
if not _sane(last_close):
    # 向前扫历史，直到找到一根合理的 bar
    ...

# 2. 加载 oracle 状态，如果与 last_close 偏离 > 5% 则强制重锚
diverged = saved_index <= 0 or abs(saved_index - last_close) / last_close > 0.05
if diverged:
    oracle.index_price = oracle.mu = oracle.theta = last_close
    oracle.trend_bias = 0; oracle.recent_return = 0
else:
    oracle.index_price = saved_index
    oracle.mu          = saved_mu
    oracle.theta       = saved_theta
    # ... + trend_bias / recent_return / sigma / state

engine.mid_price = engine.last_price = last_close
```

**关键修复**：原代码用 `abs(saved - last) / last_close > 0.05`，当 `last_close` 为负数时不等式恒为假，会把已损坏的锚点保留下来；现在加了 `saved_index <= 0` 短路和 `_sane()` 过滤。

### 4.2 重生成路径（首次启动或 saved 过期）

```python
# 1. 30 天 × 6 ticks/min × 1440 min = 259,200 个 GBM tick
# 2. 叠加 30~50 个高波动时段（× 1.8–5.0）+ 40~70 个跳空（σ=1.3%）
# 3. 聚合到 1m / 5m / 15m / 1h / 4h / 1D 六套 K 线
# 4. engine.mid_price = engine.last_price = last_c
# 5. 【关键修复】oracle 锚点也对齐到 last_c：
oracle.index_price = oracle.mu = oracle.theta = seed_price
oracle.trend_bias  = 0.0
oracle.recent_return = 0.0
```

> 原 bug：重生成后 `engine.mid_price` 是随机游走终点（可能 14k–38k），但 `oracle.index_price` 还停留在初始化的 23500。套利者第一个 tick 看到 `deviation` 高达 ±30%，触发大单扫盘 → K 线飞涨。修复后两者强制对齐。

### 4.3 状态修复工具

[`fix_state.py`](fix_state.py)：当 `state.json` 已被污染（出现负价、巨幅振幅），运行：

```
python fix_state.py
```

会做：
1. 备份到 `state.json.bak`
2. 每个 TF 倒序扫描 history，找最近一根 `_sane`（OHLC ∈ (100, 1e6) 且 high/low < 1.10）的 bar
3. 丢弃之后所有污染 bar，清空 `current`
4. 用 1m 的 close 重锚 oracle (`index_price = mu = theta`)
5. 重置 `sigma_t = 0.00025`、`trend_bias = recent_return = 0`、`market_state = calm`

---

## 5. Oracle 价格模型

文件：[oracle.py](oracle.py)，每 **100 ms** 执行一次 `tick()`。

### 5.1 锚点自适应（防止 mu/theta 漂移过远）

```python
async def tick(self):
    if self.index_price > 100:
        if abs(self.mu - self.index_price) / self.index_price > 0.10:
            self.mu = self.index_price          # 强制把 mu 拉回当前价
        if abs(self.theta - self.index_price) / self.index_price > 0.10:
            self.theta = self.index_price       # theta 同理
```

10% 的硬阈值意味着：即使罕见的市场剧烈走势让 `index_price` 偏离 `mu/theta`，下一次 tick 会立即重新锚定，不会无限拉大偏差。

### 5.2 五分量价格公式

```python
new_price = mu
          + momentum_contrib × index_price
          + jump_contrib
          + micro_noise
new_price = max(new_price, 100.0)        # 价格下限保护
```

| 分量 | 数值范围 | 作用 |
|------|---------|------|
| `mu` | 价格主体（≈ index_price） | OU 均值回归，慢漂移 |
| `momentum_contrib × index_price` | trend_bias ≤ 0.005 → 最多 ±0.75 % | 短期动量延续 |
| `jump_contrib` | direction × magnitude × index_price | 跳空事件，幅度 0.2 %–4 % |
| `micro_noise` | ± index_price × 0.0001 | 微观抖动 |

### 5.3 GARCH(1,1) 波动率

```python
sigma2 = omega + alpha × r²_{t-1} + beta × sigma2_{t-1}
       = 3e-9  + 0.10 × r²       + 0.85 × sigma2
sigma2 ∈ [1e-10, 1e-6]
sigma_t = sqrt(sigma2)
```

- 长期均值方差：`omega / (1 - alpha - beta) = 6e-8`，对应 `sigma_t ≈ 0.00025`
- `alpha=0.10`：对冲击敏感
- `beta=0.85`：持久性强，半衰期 ~45 个 tick（4.5 秒）
- 硬上限 `1e-6` 防止 GARCH 失稳爆炸

### 5.4 OU 均值回归

```python
ou_drift = kappa × (theta - mu) = 0.002 × (theta - mu)
ou_noise = sigma_ou × state_vol × randn() = 0.0008 × (sigma_t × state_vol_mult) × randn()
mu += ou_drift + ou_noise
```

`kappa = 0.002` 极慢，半衰期约 350 个 tick（35 秒）。

### 5.5 动量项（带距离衰减）

```python
deviation       = abs(index_price - theta) / theta
momentum_decay  = max(0, 1 - deviation × 10)         # 价格越远离锚点，动量越弱
trend_bias      = 0.82 × trend_bias + 0.08 × recent_return
trend_bias     *= momentum_decay
momentum_contrib = trend_bias × state_momentum_mult
```

- `momentum_alpha = 0.82`：约 13 个 tick（1.3 秒）残余 < 10 %
- `state_momentum_mult`：calm 0.5 / trend 1.5 / volatile 0.8 / panic 0.3
- 距离衰减意味着价格离 theta 10% 时动量直接归零，**杜绝了"越涨越涨"的正反馈**

### 5.6 跳空过程

```python
jump_lambda = 0.0005 × state_jump_mult
if random() < jump_lambda:
    is_down  = random() < 0.55                      # 下跌偏置 55 %
    is_large = random() < 0.06                      # 6 % 概率出大跳
    magnitude = uniform(0.01, 0.04) if is_large else uniform(0.002, 0.008)
    jump_contrib = direction × magnitude × index_price
    shock_recovery_ticks = 24                       # 24 tick 内偏向 calm/trend
```

**单次跳空硬上限 4 %**（`is_large=True` 时 magnitude ≤ 0.04）。同时广播 `oracle.shock` 事件，做市商可选择立即撤单。

| 状态 | jump_mult | 跳空概率 / tick | 平均间隔 |
|------|-----------|----------------|---------|
| calm | 0.3 | 0.015 % | 67 秒 |
| trend | 0.8 | 0.04 % | 25 秒 |
| volatile | 1.5 | 0.075 % | 13 秒 |
| panic | 3.0 | 0.15 % | 7 秒 |

---

## 6. 市场状态机

每 **24 个 oracle tick**（2.4 秒）评估一次。

### 6.1 转移矩阵

| 当前 → | calm | trend | volatile | panic |
|---|---|---|---|---|
| **calm** | 97.0 % | 2.0 % | 0.8 % | 0.2 % |
| **trend** | 5.0 % | 92.0 % | 2.5 % | 0.5 % |
| **volatile** | 11.5 % | 15.5 % | 71.5 % | 1.5 % |
| **panic** | 20.0 % | 10.0 % | 30.0 % | 40.0 % |

### 6.2 状态效果乘数

| 状态 | vol_mult | jump_mult | momentum_mult | 平均持续 |
|------|---------|-----------|---------------|---------|
| calm | 0.5× | 0.3× | 0.5× | ~3.2 分钟 |
| trend | 1.0× | 0.8× | 1.5× | ~1 分钟 |
| volatile | 2.0× | 1.5× | 0.8× | ~17 秒 |
| panic | 4.0× | 3.0× | 0.3× | ~6 秒 |

### 6.3 跳空恢复偏置

跳空后 24 个 evaluation 内，转移矩阵会被偏向 `calm/trend`（`calm/trend +0.05`、`panic -0.10`），归一化后选下一状态。**杜绝跳空后 panic 状态自我维持**。

---

## 7. 订单簿与撮合引擎

文件：[engine.py](engine.py)。

### 7.1 数据结构

```python
self.asks = SortedDict()                # 卖单：价格升序
self.bids = SortedDict(lambda x: -x)    # 买单：价格降序（key 取反实现）
# 每个 price level 一个 list[Order]，FIFO
```

### 7.2 mid_price 更新

```python
def _update_mid(self):
    best_ask = next(iter(self.asks), None)
    best_bid = next(iter(self.bids), None)
    if best_ask and best_bid:
        self.mid_price = (best_ask + best_bid) / 2
```

仅在 **限价单插入** 或 **成交** 时调用。单边为空时 mid 保持上次值。

### 7.3 市价单匹配（含空簿重试）

```python
async def _match_market(self, order):
    book = self.asks if order.side == 'buy' else self.bids

    # 用户单：如果对面簿为空，最多重试 3 次 × 50 ms
    # 给做市商一个补报价的窗口
    if not book and order.owner == 'user':
        for _ in range(3):
            await asyncio.sleep(0.05)
            book = self.asks if order.side == 'buy' else self.bids
            if book: break

    # 重试后仍空 → 发布 order.rejected 事件
    if not book:
        await bus.publish('order.rejected', {
            'reason': 'no_liquidity',
            'order_id': order.order_id,
            'owner': order.owner,
            'side': order.side,
            'size': order.size,
        })
        return []

    # ... 否则按价格档遍历填单 ...
```

**Agent 内部下单（owner != 'user'）不重试**，立即返回空，避免阻塞撮合循环。

### 7.4 自成交防护

```python
if passive.owner == order.owner:
    continue   # 同账户的对手单跳过
```

### 7.5 止损单重入保护

`_match_market` 内部触发的止损单 → 收集到 `_deferred_stops` → 等最外层 `submit_order` 返回后再处理。避免嵌套撮合破坏订单簿迭代器。

### 7.6 用户单事件链

| 事件 | 来源 | 订阅方 |
|------|------|--------|
| `engine.trade` | 每次 `_execute_fill` | WSPush（前端 trade tape）、TradeJournal |
| `order.rejected` | 空簿且重试失败 | WSPush（写日志区"暂无流动性"）+ `/api/close` API（返回 `no_liquidity`） |
| `user.impact` | 用户作为 aggressor 成交 | Oracle（叠加 trend_bias） |

---

## 8. 做市商报价逻辑

文件：[agents/market_maker.py](agents/market_maker.py)。两个实例：mm_aggressive（每 100 ms）和 mm_conservative（每 150 ms）。

### 8.1 基础参数（mainstream 预设）

| 参数 | mm_aggressive | mm_conservative |
|------|--------------|----------------|
| `base_spread_pct` | 0.04 % | 0.12 % |
| `n_levels` | 25 | 20 |
| 内档间距 | 1.0 USDT | 2.0 USDT |
| 外档间距 | 2.5 USDT | 5.0 USDT |
| 内档基础量 | 0.5 AEN | 1.5 AEN |
| 外档基础量 | 2.0 AEN | 8.0 AEN |
| `max_inventory` | 5,000 AEN | 8,000 AEN |
| `skew_factor` | 0.3 | 0.2 |

### 8.2 实时半价差

```python
vol_mult    = min(1 + sigma_t × 500, 3.0)       # 上限 3×
state_mult  = {calm: 0.7, trend: 1.0, volatile: 1.8, panic: 3.5}[state]
half_spread = mid × base_spread_pct/2 × vol_mult × state_mult × spread_multiplier × startup_mult
```

`startup_mult` 在冷启动后 ~2.5 秒内从 2.5 → 2.0 → 1.5 → 1.2 → 1.0 渐进收敛。

### 8.3 库存偏移（skew）— 安全修订版

```python
# inv_ratio 截到 ±1，防止库存超限时 skew 放大到 mid 量级
inv_ratio = max(-1.0, min(self.inventory / cfg['max_inventory'], 1.0))
skew      = inv_ratio × cfg['skew_factor'] × mid     # 上限 ±0.3·mid（aggressive）

bid_base = mid - half_spread - skew
ask_base = mid + half_spread - skew

# 兜底正价：报价绝对不能跌破 mid×0.5
bid_base = max(bid_base, mid × 0.5)
ask_base = max(ask_base, mid × 0.5)
```

| 库存方向 | skew | 报价整体位移 | 效果 |
|---------|------|------------|------|
| 多头超额（inv > 0） | 正 | bid/ask 整体下移 | 倾向卖出，去库存 |
| 空头超额（inv < 0） | 负 | bid/ask 整体上移 | 倾向买入，去库存 |

**关键修复**：旧版没有 `inv_ratio` 截断，库存极端时 skew 可超过 mid（30 % × 5 = 150 % mid），导致 `bid_base` 为负。新版双保险：截 `inv_ratio` + 兜底 `max(..., mid × 0.5)`。

### 8.4 OFI 调整

```python
# 买方主导（ofi 高）→ ask 量大，bid 量小
if ofi > ofi_sensitivity:
    ofi_adj = (ofi - ofi_sensitivity) × 0.5
elif ofi < (1 - ofi_sensitivity):
    ofi_adj = -(ofi_sensitivity - ofi) × 0.5

bid_size = base × (1 - max(0, ofi_adj))
ask_size = base × (1 + max(0, ofi_adj))
```

只调整每档单量，**不改报价**。

### 8.5 撤单/重报顺序

`_post_quotes` 第一行调用 `_cancel_all()`，**先全部撤单再挂新单**。这保证报价中心始终跟随最新 `mid_price`，不会出现"挂出后 100 ms 不动"的情况。

### 8.6 紧急对冲

当 `abs(inventory) >= max_inventory` 时进入 `_emergency_hedge()`：撤掉双边挂单，仅在过剩方向布 25 档限价（多则全卖、空则全买）。直到库存回到阈值内才恢复双边做市。

### 8.7 跳空响应

订阅 `oracle.shock` → 立即 `_cancel_all`（aggressive；conservative 可选保留），`shock_cooldown = 3` 个 tick，恢复时用 2× 价差。

---

## 9. 套利者

文件：[agents/arbitrageur.py](agents/arbitrageur.py)，每 100 ms 触发。

### 9.1 偏差分级

```python
deviation = (mid - index_price) / index_price
```

| 偏差区间 | 动作 |
|---------|------|
| `[0, 0.15%)` | 不动 |
| `[0.15%, 0.30%)` | 在 `index ± 0.05%` 挂限价单 |
| `[0.30%, 5%)` | 市价单扫盘（被限制单量） |
| `≥ 5%` | **直接 return**，认定锚点异常拒绝套利 |

### 9.2 单量计算（截尾后）

```python
# 旧版无上限，0.32 的 deviation 会把 aggression 放大到 200+
aggression = min((|dev| - entry_threshold) / entry_threshold, 5.0)   # 上限 5
base_size  = 50 × (1 + aggression × 3)                                # 上限 800
base_size  = min(base_size, max_inventory - |inventory|)              # 不能超库存
if |dev| > market_order_threshold:
    base_size = min(base_size, 100.0)                                 # 市价单每 tick 上限 100
```

**多重安全阀**说明：
1. `aggression ≤ 5`：防止极端偏差时单量爆炸式增长
2. `≤ 5% deviation` 拦截：> 5% 几乎总是锚点错位（冷启动残留、状态污染），市价扫盘只会加剧问题
3. 市价单 100 AEN 上限：每 100 ms 最多扫 100 AEN，避免单次穿透多档报价

这三层把 **16k → 36k 那种单次大单扫飞** 彻底封死。

### 9.3 资金费率套利

`|funding_rate| > 0.03 %` 时，在 `mid × (1 ∓ 0.02 %)` 挂单 30 AEN（限价单，不主动扫盘）。

---

## 10. 趋势跟随者与噪声交易者

### 10.1 TrendFollower（150 ms）

```python
recent_high = max(price_history[-20:])
recent_low  = min(price_history[-20:])
if mid > recent_high × 1.001 and inventory < 2000:
    # 在 mid × 1.001 挂限价买，设 1.5% 止损
elif mid < recent_low × 0.999 and inventory > -2000:
    # 在 mid × 0.999 挂限价卖
```

收到 `oracle.shock` 且 `is_large=True`：立即市价 100 AEN 顺势入场。

### 10.2 NoiseTrader（200 ms）

```python
base_rate = {calm: 1.5, trend: 2.0, volatile: 2.0, panic: 3.5}[state]
vol_boost = 1 + sigma_t × 200
n_orders  = Poisson(base_rate × vol_boost)
```

**关键调参**：calm 从 0.5 提到 1.5、trend 从 1.0 提到 2.0，确保即使在 calm 状态每分钟也有充足成交，避免 K 线出现长时间静止段。

每笔订单：

```python
signal = randn() × 0.4
       + sign(recent_ret) × min(|recent_ret| × 100, 1) × 0.35
       + (ofi - 0.5) × 2 × 0.25
side = 'buy' if signal > 0.3 else 'sell' if signal < -0.3 else SKIP
```

55 % 市价 / 45 % 限价。单笔尺寸 `lognormal(0.5, 1.2)` 截到 [0.01, 50] AEN。

持仓管理：浮亏 2 % / 浮盈 3 % / 持有 > 300 秒 → 市价平仓。

---

## 11. 中间价与标记价

文件：[contracts/mark_price.py](contracts/mark_price.py)

### 11.1 mid_price

```
mid_price = (best_ask + best_bid) / 2
```

仅在限价插入 / 成交时更新。订单簿空单边时停止更新。

### 11.2 mark_price 计算

```python
# 1. 采样：每次 oracle.tick 记录 (now, mid)，前提：
#    - mid > 0
#    - |mid - index| / index < 20%   ← 防污染采样
#    - 窗口 30 分钟，过期自动丢弃

async def _on_oracle_tick(self, data):
    self.index_price = data['index_price']
    mid = self.engine.mid_price
    if mid > 0 and (index <= 0 or abs(mid - index) / index < 0.20):
        self.mid_history.append((now, mid))
        # 滚动窗口
        self.mid_history = [(t, p) for t, p in self.mid_history if t > now - 1800]

# 2. compute()：线性权重 TWAP
def compute(self):
    if not self.mid_history or self.index_price <= 0:
        return self.index_price
    weighted_sum = Σ px × (1 - (now - ts) / 1800)
    weight_total = Σ      (1 - (now - ts) / 1800)
    twap  = weighted_sum / weight_total
    basis = twap - self.index_price
    # 关键修复：basis 夹到 ±0.5%·index_price
    basis = clamp(basis, -index × 0.005, index × 0.005)
    return self.index_price + basis
```

**修复细节**：
- 原公式 `mark = index + (twap - index) = twap`，basis 无上限，K 线污染期间 mid 拉到 36k 会把 TWAP 拖到 9k 区，mark_price 跟着脱锚
- 现在 basis 被 ±0.5 % 截断 + 采样阶段就拒绝 |mid-index|/index > 20% 的脏数据
- index ≤ 0 时直接返回 index（避免除零）

### 11.3 spread / ofi

```python
spread = best_ask - best_bid
ofi    = sum(size for size, side='buy' in last 200 trades) / total
```

---

## 12. 用户大单冲击

文件：[engine.py](engine.py) `_execute_fill` 末尾。

```python
if aggressor.owner == 'user':
    await bus.publish('user.impact', {
        'net_qty': size,
        'direction': 1 if side == 'buy' else -1,
        'impact_magnitude': size × price,
    })
```

订阅方 [`oracle.apply_user_impact`](oracle.py)：

```python
impact = (net_qty × index_price) / 1e7
impact = min(impact, 0.002)                # 单笔冲击上限 0.2%
trend_bias += direction × impact
```

后续传导链：

```
trend_bias 叠加
    ↓ 每 100ms oracle tick
momentum_contrib × index_price
    ↓
recent_return 变化 → GARCH sigma_t 抬升
    ↓
做市商价差扩宽 + skew 微调
    ↓
可能切换到 volatile/panic 状态
    ↓
更多跳空 / 趋势跟随者追单 / 噪声 vol_boost
```

`trend_bias` 衰减系数 `0.82`，约 13 个 tick（1.3 秒）残余 < 10 %。

---

## 13. 资金费率与清算

### 13.1 资金费率（每 30 秒更新，每 1 小时结算）

```python
premium_index = (mark_price - index_price) / index_price
rate = premium_index + clamp(0.0001 - premium_index, -0.0005, 0.0005)
rate = clamp(rate, -0.0075, +0.0075)         # ±0.75%
```

- mark > index → rate > 0，多付空
- mark < index → rate < 0，空付多

结算时直接加减 `pos['margin']`，跌至 0 触发清算。

### 13.2 清算

```python
liq_price = entry × (1 - 1/leverage + 0.01)   # long
liq_price = entry × (1 + 1/leverage - 0.01)   # short
```

触发后市价平仓，0.5 % 清算费。损失超过 margin 时由 `insurance_fund`（初始 50,000 USDT）兜底，耗尽则启动 ADL（自动减仓）。

---

## 14. K 线构建

文件：[engine.py](engine.py) `KlineBuilder`。

每次 `_execute_fill` 触发后，6 个 `KlineBuilder`（1m/5m/15m/1h/4h/1D）同步更新：

```python
def update(self, price, volume):
    bar_start = floor(now / seconds) × seconds      # 按整点对齐
    if self.current is None or self.current['t'] != bar_start:
        if self.current:
            history.append(dict(self.current))      # 旧 bar 入历史
            if len(history) > 1000: history.pop(0)  # 上限 1000 根
        self.current = {'t': bar_start, 'tf': tf,
                        'o': price, 'h': price, 'l': price, 'c': price, 'v': 0}
    self.current['h'] = max(h, price)
    self.current['l'] = min(l, price)
    self.current['c'] = price
    self.current['v'] += volume
```

WebSocket 推送时每个 TF 取最近 200 根 + current。

---

## 15. 持久化

文件：[persistence.py](persistence.py)，写到 `./data/state.json`。

### 15.1 保存字段

```json
{
  "balance": 1000000.0,
  "insurance_fund": 50000.0,
  "klines": {
    "1m": {"history": [...200], "current": {...}},
    ...
  },
  "oracle": {
    "index_price": ..., "mu": ..., "theta": ...,
    "sigma_t": ..., "sigma2": ...,
    "trend_bias": ..., "recent_return": ...,
    "market_state": "calm"
  },
  "journal": [...最近 500 条],
  "timestamp": 1778266380.5
}
```

### 15.2 加载顺序（在 main.py）

```
load_state()              # 恢复 oracle 全量 + klines + balance + journal
  ↓
cold_start()
  ├─ 跳过路径：     用 last_close 校验 + 重锚（如果偏离 > 5%）
  └─ 重生成路径：   30 天历史 + 强制对齐 oracle = engine.mid_price
  ↓
clock.run()
```

每 60 秒（1200 tick）自动 `periodic_save()` 一次。

---

## 16. 安全阀汇总（防飞涨）

下表是把上文散落各处的安全阀集中列出，方便排查：

| # | 位置 | 阀门 | 触发条件 | 动作 |
|---|------|------|---------|------|
| 1 | oracle.tick | mu 重锚 | `|mu - index|/index > 10%` | `mu = index` |
| 2 | oracle.tick | theta 重锚 | `|theta - index|/index > 10%` | `theta = index` |
| 3 | oracle.tick | GARCH 上限 | `sigma2 > 1e-6` | 截断到 1e-6 |
| 4 | oracle.tick | 价格下限 | `index < 100` | 截到 100 |
| 5 | oracle.tick | 动量衰减 | `|index-theta|/theta > 10%` | momentum_decay → 0 |
| 6 | _generate_jump | 跳空上限 | — | 单次 ≤ 4% |
| 7 | _update_state | 跳空后偏置 | shock_recovery_ticks > 0 | 偏向 calm/trend |
| 8 | cold_start (skip) | 锚点校验 | `saved_index ≤ 0` 或 偏离 > 5% | 重锚到 last_close |
| 9 | cold_start (skip) | 污染过滤 | `last_close ∉ (100, 1e6)` | 倒查 history |
| 10 | cold_start (regen) | **必须对齐** | — | `oracle = engine.mid_price = last_c` |
| 11 | _match_market | 空簿重试 | 用户单 + book 为空 | 3 × 50 ms 重试 |
| 12 | _match_market | 拒单事件 | 重试后仍空 | publish `order.rejected` |
| 13 | arbitrageur | 偏差上限 | `|deviation| > 5%` | 直接 return |
| 14 | arbitrageur | aggression 上限 | — | ≤ 5 |
| 15 | arbitrageur | 市价单上限 | `|dev| > 0.3%` | ≤ 100 AEN/tick |
| 16 | market_maker | inv_ratio 截断 | — | clamp(±1) |
| 17 | market_maker | 报价兜底 | bid/ask < mid×0.5 | clamp 到 mid×0.5 |
| 18 | mark_price | 采样过滤 | `|mid-index|/index ≥ 20%` | 不进 TWAP |
| 19 | mark_price | basis 截断 | — | clamp(±0.5%·index) |
| 20 | user.impact | 冲击上限 | — | ≤ 0.2% |
| 21 | funding | 费率上限 | — | clamp(±0.75%) |

---

## 17. 三种预设市场

由环境变量 `TWINS_PRESET` 控制（默认 `mainstream`）：

| 预设 | spread (激进/保守) | oracle_vol_mult | 噪声倍率 | 套利门槛 | 初始状态 |
|------|-----------------|----------------|---------|---------|---------|
| `retail` | 0.3 % / 0.8 % | 1.5× | 0.8× | 0.5 % | calm |
| `mainstream`（默认）| 0.04 % / 0.12 % | 1.0× | 1.0× | 0.15 % | calm |
| `extreme` | 0.8 % / 2.0 % | 4.0× | 2.5× | 0.5 % | **panic** |

---

## 附：常见问题诊断

| 症状 | 检查 |
|------|------|
| K 线突然大幅跳涨/跳跌 | 1. `oracle.index_price` vs `engine.mid_price` 差是否 > 5%（套利者会扫盘）；2. `fix_state.py` 跑过没；3. 当前 state 是否 `panic` |
| 标记价与指数价偏离明显 | 1. `mid_history` 是否被脏 mid 污染；2. basis 限幅 ±0.5% 是否合适当前预设 |
| 用户点平仓无反应 | 1. F12 控制台看是否走到 `/api/close`；2. 后端日志看 `order.rejected` 是否触发；3. 是否对面簿确实长时间为空（怀疑 MM 出错） |
| K 线相邻 bar 跳价 | 频率应该不会再有问题；如果仍有跳价 > 0.3%，看是否在跳空事件附近（正常） |
| K 线影线虚长 | 在 calm 状态下，oracle 自然 ±0.3–0.6% 振幅、净位移近零，影线/实体比 5–15 是正常的，不是 bug |

---

*文档对应代码版本：2026-05 修复全套安全阀 + agent 提频 + 市价单空簿重试。*
