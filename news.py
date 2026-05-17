import uuid
import time
import random


NEWS_TEMPLATES = [
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

NEWS_PRICE_NAMES = {
    'price': lambda: f"{random.randint(22000, 25000):,}",
}


class NewsSystem:
    def __init__(self):
        self.news_history: list[dict] = []
        self.last_news_time = 0.0
        self.min_interval = 45
        self.max_interval = 180
        self._tick_count = 0

    async def tick(self, market_state: str, price_change_pct: float):
        self._tick_count += 1

        # Only evaluate every 20 ticks (1 second) to avoid excessive checks
        if self._tick_count % 20 != 0:
            return

        now = time.time()
        if now - self.last_news_time < self.min_interval:
            return

        base_prob = 0.02
        if market_state in ('volatile', 'panic'):
            base_prob = 0.05
        if abs(price_change_pct) > 0.02:
            base_prob = 0.08

        if random.random() > base_prob:
            return

        self.last_news_time = now

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
        if not candidates:
            candidates = [t for t in NEWS_TEMPLATES if t[3] is None]

        tag, source, template, _ = random.choice(candidates)

        text = template
        for key in FILL_VALUES:
            placeholder = '{' + key + '}'
            if placeholder in text:
                val = FILL_VALUES[key]
                if callable(val):
                    val = val()
                else:
                    val = random.choice(val)
                text = text.replace(placeholder, val)

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
