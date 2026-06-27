import os
import ccxt.pro as ccxt
from loguru import logger

class UniversalCEXTrader:
    def __init__(self, exchange_id='okx', market_type='swap'):
        """
        通用 CEX 交易执行器
        :param exchange_id: ccxt 支持的交易所字符串 ID (如 'okx', 'binance', 'bybit')
        :param market_type: 'spot' 代表现货，'swap' 代表永续合约
        """
        self.exchange_id = exchange_id
        self.market_type = market_type
        
        # 动态获取对应的 ccxt 类
        exchange_class = getattr(ccxt, exchange_id)
        
        # 根据交易所动态拉取对应的环境变量密钥
        prefix = exchange_id.upper()
        exchange_config = {
            'apiKey': os.getenv(f"{prefix}_API_KEY"),
            'secret': os.getenv(f"{prefix}_SECRET"),
            'password': os.getenv(f"{prefix}_PASSWORD"),
            'enableRateLimit': True,
            "aiohttp_proxy": "http://127.0.0.1:7892",
            'options': {
                'defaultType': market_type,
                'x-simulated-trading': '1'
            }
        }
        proxy_url = os.getenv("HTTP_PROXY")
        if proxy_url:
            exchange_config['proxies'] = {
                'http': proxy_url,
                'https': proxy_url,
                "aiohttp_proxy": proxy_url,
            }
        self.exchange = exchange_class(exchange_config)
        use_sandbox = os.getenv("USE_EXCHANGE_TESTNET", "False").upper() == "TRUE"
        if use_sandbox:
            self.exchange.set_sandbox_mode(True)
            logger.warning(f"[{self.exchange_id}] 已切入官方 Testnet 模拟盘")
            
        
        self.markets_info = {}

    async def initialize(self):
        self.markets_info = await self.exchange.load_markets()
        logger.info(f"⚡ 通用交易组件：[{self.exchange_id}] 元数据加载成功 ({self.market_type} 模式)。")

    def _format_symbol(self, symbol: str) -> str:
        """确保合约使用正确的 CCXT 标准后缀 (如 BTC/USDT:USDT)"""
        if self.market_type == 'swap' and ':' not in symbol:
            quote = symbol.split('/')[1]
            return f"{symbol}"
        return symbol

    async def get_balance_usdt(self):
        balance = await self.exchange.fetch_balance()
        return balance['free'].get('USDT', 0.0)

    async def get_balance_usdt(self):
        balance = await self.exchange.fetch_balance()
        return balance['free'].get('USDT', 0.0)

    async def buy(self, symbol: str, amount_usdt: float, take_profit: float = None, stop_loss: float = None, td_mode: str = 'isolated'):
        """
        通用市价买入/开多 (支持止盈止损)
        :param td_mode: 保证金模式 ('isolated' 逐仓 或 'cross' 全仓) - OKX 统一账户必填
        """
        try:
            formatted_symbol = symbol
            
            # 1. 获取当前最新价格用于计算数量
            ticker = await self.exchange.fetch_ticker(formatted_symbol)
            raw_price = ticker.get('last') or ticker.get('close') or ticker.get('ask') or ticker.get('bid')
            
            if raw_price is None:
                logger.error(f"[{self.exchange_id}] 无法获取 {formatted_symbol} 的有效价格")
                return None
                
            current_price = float(raw_price)
            
            # 2. 获取市场元数据并计算目标数量
            market = self.exchange.market(formatted_symbol)
            
            if self.market_type == 'spot':
                target_amount = amount_usdt / current_price
            else:
                raw_contract_size = market.get('contractSize', 1.0)
                contract_size = float(raw_contract_size if raw_contract_size is not None else 1.0)
                target_amount = amount_usdt / (current_price * contract_size)

            # 3. 按照交易所规定的精度截断
            raw_order_qty = self.exchange.amount_to_precision(formatted_symbol, target_amount)
            if not raw_order_qty:
                logger.error(f"[{self.exchange_id}] 精度截断失败 {formatted_symbol}")
                return None
                
            order_qty = float(raw_order_qty)
            if order_qty <= 0:
                logger.error(f"[{self.exchange_id}] 计算得出的下单数量为 0")
                return None

            # 4. 组装附加参数 params
            params = {
                "posSide": "long"
            }
            if self.exchange_id == 'okx' and self.market_type == 'swap':
                params['tdMode'] = td_mode  # OKX 统一账户核心参数
                
                if take_profit:
                    params['tpTriggerPx'] = str(take_profit)
                    params['tpOrdPx'] = '-1'  # 触发后市价平仓
                
                if stop_loss:
                    params['slTriggerPx'] = str(stop_loss)
                    params['slOrdPx'] = '-1'  # 触发后市价平仓

            logger.info(f"[{self.exchange_id}] 执行市价买入: {formatted_symbol} | 预估金额: {amount_usdt} USDT | 下单量: {order_qty}")
            
            # 使用 create_order 替代 create_market_order，更方便透传扩展 params
            order = await self.exchange.create_order(
                symbol=formatted_symbol,
                type='market',
                side='buy',
                amount=order_qty,
                price=None,  # 市价单无需价格
                params=params
            )
            
            # 兜底回执数据
            if not order.get('average') and not order.get('price'):
                order['price'] = current_price
            if not order.get('filled') and not order.get('amount'):
                order['amount'] = order_qty
            
            logger.success(f"[{self.exchange_id}] 买入成功: {formatted_symbol} | 单号: {order['id']}")
            return order
            
        except Exception as e:
            logger.error(f"[{self.exchange_id}] 买入失败 {symbol}: {e}")
            return None

    async def sell(self, symbol: str, percentage: float = 1.0, td_mode: str = 'isolated'):
        """通用市价卖出/平多"""
        try:
            formatted_symbol = symbol
            sell_qty = 0.0
            params = {}

            if self.market_type == 'spot':
                balance = await self.exchange.fetch_balance()
                base_asset = symbol.split('/')[0]
                total_holding = balance['total'].get(base_asset, 0.0)
                sell_qty = total_holding * percentage
            else:
                positions = await self.exchange.fetch_positions([formatted_symbol])
                if not positions:
                    logger.warning(f"[{self.exchange_id}] 未找到 {formatted_symbol} 的持仓记录")
                    return False
                    
                pos = positions[0] 
                total_holding = float(pos['contracts'])
                sell_qty = total_holding * percentage
                
                params['reduceOnly'] = True
                if self.exchange_id == 'okx':
                    params['tdMode'] = td_mode

            sell_qty_formatted = float(self.exchange.amount_to_precision(formatted_symbol, sell_qty))
            
            if sell_qty_formatted <= 0:
                logger.warning(f"[{self.exchange_id}] {formatted_symbol} 无可用余额或仓位")
                return False
                
            logger.info(f"[{self.exchange_id}] 执行市价卖出/平多: {formatted_symbol} | 卖出数量: {sell_qty_formatted}")
            
            order = await self.exchange.create_order(
                symbol=formatted_symbol,  
                type='market',
                side='sell', 
                amount=sell_qty_formatted,
                price=None,
                params=params
            )
            
            logger.success(f"[{self.exchange_id}] 卖出成功: {formatted_symbol} | 单号: {order['id']}")
            return True
            
        except Exception as e:
            logger.error(f"[{self.exchange_id}] 卖出失败 {symbol}: {e}")
            return False

    async def close(self):
        await self.exchange.close()