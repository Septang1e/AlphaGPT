import asyncio
import os
from datetime import datetime, timedelta
import ccxt.async_support as ccxt
from loguru import logger

from ..config import Config
from .base import DataProvider

class OKXProvider(DataProvider):
    def __init__(self):
        # 1. 动态加载交易所配置，开启官方直连防DNS污染
        exchange_config = {
            'enableRateLimit': True,
            'hostname': 'aws.okx.com',
        }
        
        # 2. 注入代理配置（兼容环境变量）
        proxy_url = os.getenv("HTTP_PROXY")
        if proxy_url:
            exchange_config['proxies'] = {
                'http': proxy_url,
                'https': proxy_url
            }
            exchange_config['aiohttp_proxy'] = proxy_url
            
        self.exchange = ccxt.okx(exchange_config)
        self.exchange_id = 'okx'
        
        # 限制并发量，防止触发 OKX 封控
        self.semaphore = asyncio.Semaphore(Config.CONCURRENCY)

    async def get_trending_tokens(self, limit=100):
        """
        获取 OKX 交易量最大的 Top N 个 USDT 交易对作为候选池。
        在 CEX 语境下，我们将 symbol (如 BTC/USDT) 直接映射为数据库里的 address。
        """
        try:
            await self.exchange.load_markets()
            tickers = await self.exchange.fetch_tickers()
            
            valid_tickers = []
            for symbol, ticker in tickers.items():
                # 筛选活跃的 USDT 现货或永续合约
                if 'USDT' in symbol and ticker.get('quoteVolume') is not None:
                    valid_tickers.append(ticker)
            
            # 按 24 小时成交额（USDT计价）降序排列，这就是 CEX 的 "Trending"
            valid_tickers.sort(key=lambda x: x['quoteVolume'], reverse=True)
            top_tickers = valid_tickers[:limit]
            
            results = []
            for t in top_tickers:
                symbol = t['symbol']
                results.append({
                    'address': symbol,             # 借用 address 字段存储 symbol
                    'symbol': symbol,
                    'name': symbol.split('/')[0],
                    'decimals': 4,                 # 默认精度，仅作入库占位
                    'liquidity': t['quoteVolume'], # CEX 没有底层池子流动性，用 24h 交易额替代
                    'fdv': t['quoteVolume'] * 10   # 占位估算
                })
                
            logger.info(f"[{self.exchange_id}] 成功获取 {len(results)} 个高热度交易对作为数据池")
            return results
        except Exception as e:
            logger.error(f"[{self.exchange_id}] 获取热门交易对失败: {e}")
            return []

    async def get_token_history(self, session, address, days=Config.HISTORY_DAYS, liquidity=None, fdv=None):
        """
        获取单个交易对的历史 K 线数据。
        注：这里的 session 参数是为了兼容 DataProvider 基类接口，CCXT 自带请求池，直接忽略。
        """
        symbol = address 
        since = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
        
        async with self.semaphore:
            for attempt in range(3):
                try:
                    # 拉取历史 OHLCV。OKX 单次最多 100 条，CCXT 底层会自动处理 pagination。
                    # 如果 Config.HISTORY_DAYS 比较大（比如7天，1m周期即10080条），这里拉取时间会稍微长一点
                    ohlcvs = await self.exchange.fetch_ohlcv(symbol, Config.TIMEFRAME, since=since)
                    
                    if not ohlcvs:
                        return []
                        
                    formatted = []
                    snapshot_liq = self._as_float(liquidity)
                    snapshot_fdv = self._as_float(fdv)
                    
                    for candle in ohlcvs:
                        timestamp, o, h, l, c, v = candle
                        formatted.append((
                            datetime.fromtimestamp(timestamp / 1000.0), # time[cite: 1]
                            symbol,                                     # address[cite: 1]
                            float(o),                                   # open[cite: 1]
                            float(h),                                   # high[cite: 1]
                            float(l),                                   # low[cite: 1]
                            float(c),                                   # close[cite: 1]
                            float(v),                                   # volume[cite: 1]
                            snapshot_liq,                               # liquidity (快照)[cite: 1]
                            snapshot_fdv,                               # fdv (快照)[cite: 1]
                            self.exchange_id                            # source[cite: 1]
                        ))
                    return formatted
                    
                except ccxt.RateLimitExceeded:
                    logger.warning(f"[{self.exchange_id}] 触发速率限制 ({symbol})，休眠后重试...")
                    await asyncio.sleep(2)
                except Exception as e:
                    logger.warning(f"[{self.exchange_id}] 获取 {symbol} K线异常: {e} (尝试 {attempt+1}/3)")
                    await asyncio.sleep(1)
            
            logger.error(f"[{self.exchange_id}] 获取 {symbol} 数据彻底失败。")
            return []

    @staticmethod
    def _as_float(value, default=0.0):
        try:
            return float(value) if value is not None else default
        except (TypeError, ValueError):
            return default

    async def close(self):
        await self.exchange.close()