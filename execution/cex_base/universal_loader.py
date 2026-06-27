import asyncio
import torch
import numpy as np
import ccxt.pro as ccxt
from loguru import logger
import os

class UniversalCEXDataLoader:
    def __init__(self, exchange_id='okx', target_symbols=None):
        self.exchange_id = exchange_id
        
        # 动态加载指定的交易所类
        exchange_class = getattr(ccxt, exchange_id)
        exchange_config = {
            'enableRateLimit': True, # CCXT 内置的速率限制开关
            "aiohttp_proxy": "http://127.0.0.1:7892",
        }
        
        # 处理代理环境变量
        proxy_url = os.getenv("HTTP_PROXY")
        if proxy_url:
            exchange_config['proxies'] = {
                'http': proxy_url,
                'https': proxy_url,
                "aiohttp_proxy": proxy_url,
            }
        
        self.exchange = exchange_class(exchange_config)
        self.symbols = target_symbols or ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'ORDI/USDT', 'DOGE/USDT']
        self.feat_tensor = None
        
        # 将 token 名称映射到特征张量的索引
        self.token_map = {sym: idx for idx, sym in enumerate(self.symbols)}
        self.addresses = self.symbols  # 保持属性名与老代码 runner 兼容
        
        # 限制同时最多有 5 个请求发往交易所 API
        self.semaphore = asyncio.Semaphore(5)

    async def _fetch_single_symbol(self, symbol, timeframe, limit, max_retries=3):
        """
        内部方法：带并发控制和重试机制的单币种 K 线获取
        """
        async with self.semaphore:  # 限制并发数量
            for attempt in range(max_retries):
                try:
                    # 调用 CCXT 获取 OHLCV 数据
                    res = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=limit)
                    return res
                except ccxt.RateLimitExceeded:
                    # 处理速率限制 (HTTP 429)
                    logger.warning(f"[{self.exchange_id}] 触发速率限制 ({symbol})，休眠 2 秒后重试...")
                    await asyncio.sleep(2)
                except Exception as e:
                    # 处理其他网络或 API 错误
                    logger.warning(f"[{self.exchange_id}] 获取 {symbol} K线数据异常: {e} (尝试 {attempt + 1}/{max_retries})")
                    await asyncio.sleep(1)
            
            # 如果耗尽重试次数依然失败，返回 None
            logger.error(f"[{self.exchange_id}] 获取 {symbol} 数据彻底失败。")
            return None

    async def fetch_and_compute(self, timeframe='1m', limit=60):
        """通用并行 K 线拉取与 6 维张量矩阵组装"""
        
        # 使用优化后的单币种获取方法构建任务列表
        tasks = [self._fetch_single_symbol(sym, timeframe, limit) for sym in self.symbols]
        results = await asyncio.gather(*tasks)
        
        all_data = []
        for sym, res in zip(self.symbols, results):
            # 如果请求失败或返回为空，用 0 填充该币种的矩阵阵列，防止整个张量形状崩溃
            if res is None or not res:
                all_data.append(np.zeros((limit, 5)))
                continue
            # 截取 Open, High, Low, Close, Volume 这 5 列 (跳过时间戳)
            all_data.append(np.array(res)[:, 1:6])
            
        # 自动判断硬件环境加速 (如果有 GPU 则扔进 CUDA)
        device = "cuda" if torch.cuda.is_available() else "cpu"
        
        # 组装基础 3D 张量：形状为 [币种数量, 序列长度(时间步), 5个OHLCV特征]
        raw_tensor = torch.tensor(np.stack(all_data), dtype=torch.float32, device=device)
        
        # 维度变换：将特征移动到第 1 维 -> [Tokens, Features, Timesteps]
        raw_tensor = raw_tensor.permute(0, 2, 1) 
        
        # 拆解出独立的张量方便计算
        op, hi, lo, cl, vo = raw_tensor[:, 0, :], raw_tensor[:, 1, :], raw_tensor[:, 2, :], raw_tensor[:, 3, :], raw_tensor[:, 4, :]
        
        # 1. 收益率 (Log Return)：ln(当前收盘价 / 上一个时间步收盘价)
        ret = torch.log(cl / (torch.roll(cl, 1, dims=1) + 1e-9))
        
        # 2. 流动性得分 (Liquidity Score)：CEX 默认深度健康，全设为 1
        liq_score = torch.ones_like(cl) 
        
        # 3. 买卖压力 (Buying Pressure)：K 线实体长度相对于振幅的占比，映射到 tanh 激活函数
        pressure = torch.tanh(((cl - op) / (hi - lo + 1e-9)) * 3.0)
        
        # 4. 情绪动量 (FOMO Score)：成交量的一阶导数（变化率）的再求导，衡量放量的加速度
        vol_prev = torch.roll(vo, 1, dims=1)
        fomo = (vo - vol_prev) / (vol_prev + 1.0) - torch.roll((vo - vol_prev) / (vol_prev + 1.0), 1, dims=1)
        
        # 5. 价格偏离度 (Deviation)：当前价格偏离均价的程度
        dev = (cl - cl.mean(dim=1, keepdim=True)) / (cl.mean(dim=1, keepdim=True) + 1e-9)
        
        # 6. 对数成交量 (Log Volume)：平滑绝对成交量极值带来的影响
        log_vol = torch.log1p(vo)
        
        def robust_norm(t):
            """
            鲁棒标准化 (Robust Normalization)：
            使用中位数 (Median) 和 绝对中位差 (MAD) 进行标准化，比传统的均值/方差标准化更能抵抗极端行情（插针）的干扰。
            并将最终结果裁剪在 [-5.0, 5.0] 之间，防止特征爆炸。
            """
            median = torch.nanmedian(t, dim=1, keepdim=True)[0]
            mad = torch.nanmedian(torch.abs(t - median), dim=1, keepdim=True)[0] + 1e-6
            return torch.clamp((t - median) / mad, -5.0, 5.0)
            
        # 组装最终送入 AI 模型的特征张量矩阵
        self.feat_tensor = torch.stack([
            robust_norm(ret), 
            liq_score, 
            pressure, 
            robust_norm(fomo), 
            robust_norm(dev), 
            robust_norm(log_vol)
        ], dim=1)
        
        logger.info(self.feat_tensor)

        return self.feat_tensor

    async def close(self):
        """安全关闭交易所连接池"""
        await self.exchange.close()