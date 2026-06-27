import asyncio
import torch
import json
import os
import time
from loguru import logger
from dotenv import load_dotenv

from model_core.vm import StackVM
from strategy_manager.portfolio import PortfolioManager
from strategy_manager.risk import RiskEngine
from strategy_manager.config import StrategyConfig

from execution.cex_base.universal_loader import UniversalCEXDataLoader
from execution.cex_base.universal_trader import UniversalCEXTrader

load_dotenv()

class OKXConfiguredRunner:
    def __init__(self):
        # ─── 配置 ───
        self.EXCHANGE_ID = os.getenv("EXCHANGE_ID", 'okx')
        self.MARKET_TYPE = os.getenv("MARKET_TYPE", 'spot')
        self.MONITOR_SYMBOLS = [
            'ETH/USDT', 'SOL/USDT', 'DOGE/USDT'
        ]
        self.stop_signal_path = os.getenv("STOP_SIGNAL_PATH", f"STOP_SIGNAL_{self.EXCHANGE_ID.upper()}")
        # ──────────────────
        
        self.portfolio = PortfolioManager()
        self.risk = RiskEngine()
        self.vm = StackVM()
        
        self.loader = UniversalCEXDataLoader(exchange_id=self.EXCHANGE_ID, target_symbols=self.MONITOR_SYMBOLS)
        self.trader = UniversalCEXTrader(exchange_id=self.EXCHANGE_ID, market_type=self.MARKET_TYPE)
        
        self.symbol_map = {}
        self.last_sync_time = 0

        try:
            with open("best_meme_strategy.json", "r") as f:
                data = json.load(f)
                self.formula = data if isinstance(data, list) else data.get("formula")
            logger.success(f"[{self.EXCHANGE_ID.upper()}] 策略大脑加载成功 -> {self.formula}")
        except FileNotFoundError:
            logger.critical("未找到策略文件 'best_meme_strategy.json'！请先训练模型。")
            exit(1)

    async def initialize(self):
        await self.trader.initialize()
        usdt_bal = await self.trader.get_balance_usdt()
        logger.info(f"机器人初始化完成。[{self.EXCHANGE_ID.upper()}] 可用余额: {usdt_bal:.2f} USDT")

    async def run_loop(self):
        logger.info(f">_< | {self.EXCHANGE_ID.upper()} 策略实盘运行器已启动 (Live 模式)")
        
        while True:
            try:
                if self._handle_stop_signal(): break
                loop_start = time.time()

                # 1. 定期深度同步
                if time.time() - self.last_sync_time > 900: # 15 min
                    logger.info("o.O | 正在同步交易所市场状态...")
                    self.last_sync_time = time.time()

                # 2. 抓取实时 K 线并基于通用模块算子组装矩阵
                await self.loader.fetch_and_compute(timeframe='1m', limit=60)
                await self._build_symbol_mapping()

                if self._handle_stop_signal(): break

                # 3. 监测持仓表现 (止损、止盈、追踪止损、AI平仓)
                await self.monitor_positions()

                if self._handle_stop_signal(): break
                
                # 4. 扫描入场机会
                if self.portfolio.get_open_count() < StrategyConfig.MAX_OPEN_POSITIONS:
                    await self.scan_for_entries()
                else:
                    logger.info("=-= | 已达到最大持仓上限，跳过入场扫描。")
                
                elapsed = time.time() - loop_start
                sleep_time = max(5, 60 - elapsed)
                logger.info(f"当前周期耗时 {elapsed:.2f} 秒。休眠等待 {sleep_time:.2f} 秒...")
                await asyncio.sleep(sleep_time)
                
            except Exception as e:
                logger.exception(f"[{self.EXCHANGE_ID.upper()}] 全局循环发生错误: {e}")
                await asyncio.sleep(15)

    def _stop_requested(self):
        if not os.path.exists(self.stop_signal_path): return False
        try:
            with open(self.stop_signal_path, "r") as f:
                signal = f.read().strip().upper()
        except OSError:
            return True
        return signal in {"", "STOP", "STOPPED"}

    def _handle_stop_signal(self):
        if not self._stop_requested(): return False
        logger.warning(f"接收到 STOP 停止信号。[{self.EXCHANGE_ID.upper()}] 交易循环即将停止。")
        try:
            with open(self.stop_signal_path, "w") as f: f.write("STOPPED")
        except OSError as e:
            logger.warning(f"无法将停止信号标记为已消费: {e}")
        return True

    async def _build_symbol_mapping(self):
        self.symbol_map = {sym: idx for idx, sym in enumerate(self.loader.symbols)}

    async def monitor_positions(self):
        if not self.portfolio.positions: return
        
        logger.info(f"o.O | 正在监控 {len(self.portfolio.positions)} 个活跃持仓...")
        
        for symbol, pos in list(self.portfolio.positions.items()):

            if pos.entry_price <= 0:
                logger.warning(f"发现异常持仓数据 {symbol} (均价为 {pos.entry_price})，自动从本地监控中剔除。")
                self.portfolio.close_position(symbol)
                continue

            try:
                ticker = await self.trader.exchange.fetch_ticker(symbol)
                current_price = float(ticker['last'])
            except Exception as e:
                logger.warning(f"获取 {symbol} 价格失败: {e}")
                continue

            self.portfolio.update_price(symbol, current_price)
            pnl_pct = (current_price - pos.entry_price) / pos.entry_price
            
            # 1. 静态止损
            if pnl_pct <= StrategyConfig.STOP_LOSS_PCT:
                logger.warning(f"!!! | 触发止损: {symbol} 收益率: {pnl_pct:.2%}")
                await self._execute_sell(symbol, 1.0, "StopLoss")
                continue

            # 2. 阶段止盈 (Moonbag)
            if not pos.is_moonbag and pnl_pct >= StrategyConfig.TAKE_PROFIT_Target1:
                logger.success(f"😄 | 阶段止盈 (Moonbag): {symbol} 收益率: {pnl_pct:.2%}")
                await self._execute_sell(symbol, StrategyConfig.TP_Target1_Ratio, "Moonbag")
                pos.is_moonbag = True
                self.portfolio.save_state()
                continue

            # 3. 追踪止损 (Trailing Stop)
            max_gain = (pos.highest_price - pos.entry_price) / pos.entry_price
            drawdown = (pos.highest_price - current_price) / pos.highest_price
            
            if max_gain > StrategyConfig.TRAILING_ACTIVATION and drawdown > StrategyConfig.TRAILING_DROP:
                logger.warning(f"😠 | 追踪止损: {symbol} 最大收益: {max_gain:.2%} 回撤: {drawdown:.2%}")
                await self._execute_sell(symbol, 1.0, "TrailingStop")
                continue

            # 4. AI 动态离场
            if not pos.is_moonbag:
                ai_score = await self._run_inference(symbol)
                if ai_score != -1 and ai_score < StrategyConfig.SELL_THRESHOLD:
                    logger.info(f"🤖 | AI 动态离场: {symbol} 分数下降至: {ai_score:.2f}")
                    await self._execute_sell(symbol, 1.0, "AI_Signal")

    async def scan_for_entries(self):
        raw_signals = self.vm.execute(self.formula, self.loader.feat_tensor)
        if raw_signals is None: return

        latest_signals = raw_signals[:, -1]
        scores = torch.sigmoid(latest_signals).cpu().numpy()
        sorted_indices = scores.argsort()[::-1]
        
        idx_to_sym = {v: k for k, v in self.symbol_map.items()}
        
        for idx in sorted_indices:
            score = float(scores[idx])
            if score < StrategyConfig.BUY_THRESHOLD: break
                
            symbol = idx_to_sym.get(idx)
            if not symbol or symbol in self.portfolio.positions: continue
            
            logger.info(f"🔍 | 正在分析 {symbol} | AI 预测分: {score:.2f}")
            
            if self._handle_stop_signal(): return
            await self._execute_buy(symbol, score)
            
            if self.portfolio.get_open_count() >= StrategyConfig.MAX_OPEN_POSITIONS:
                break

    async def _execute_buy(self, symbol, score):
        usdt_balance = await self.trader.get_balance_usdt()
        order_size_usdt = self.risk.calculate_position_size(usdt_balance)
        
        if order_size_usdt <= 0.01:  # 最小下单金额保护
            logger.warning(f"余额不足以开设新仓位，或低于交易所最小下单金额({order_size_usdt} < 0.01USDT)。")
            return

        logger.info(f"🎉 | 执行买入: {symbol} | 金额: {order_size_usdt:.2f} USDT")
        
        try:
            order = await self.trader.buy(symbol, order_size_usdt)
            if order and isinstance(order, dict):
                raw_price = order.get('average') or order.get('price')
                entry_price = float(raw_price) if raw_price is not None else 0.0
                
                raw_filled = order.get('filled') or order.get('amount')
                filled_qty = float(raw_filled) if raw_filled is not None else 0.0
                
                if entry_price <= 0 or filled_qty <= 0:
                    logger.error(f"买入 {symbol} 成功但回执数据异常: 价格={entry_price}, 数量={filled_qty}，放弃录入持仓。")
                    return
                
                self.portfolio.add_position(
                    token=symbol, 
                    symbol=symbol.split('/')[0],
                    price=entry_price, 
                    amount=filled_qty, 
                    cost_sol=order_size_usdt
                )
            else:
                logger.error(f"买入 {symbol} 失败，trader 未返回有效订单回执。")
        except Exception as e:
            logger.error(f"买入 {symbol} 执行引发严重异常: {e}")

    async def _execute_sell(self, symbol, ratio, reason):
        pos = self.portfolio.positions.get(symbol)
        if not pos: return

        logger.info(f"- | 执行卖出: {symbol} | 比例: {ratio:.0%} | 原因: {reason}")
        
        try:
            success = await self.trader.sell(symbol, percentage=ratio)
            if success:
                new_amount = pos.amount_held * (1.0 - ratio)
                
                if ratio > 0.98 or new_amount * pos.entry_price < 5.0: # 如果剩余价值极低，直接清仓
                    self.portfolio.close_position(symbol)
                else:
                    self.portfolio.update_holding(symbol, new_amount)
                    
                logger.success(f"o.O | 交易完成: {symbol} 触发 {reason}")
        except Exception as e:
            logger.error(f"卖出 {symbol} 执行失败: {e}")

    async def _run_inference(self, symbol):
        idx = self.symbol_map.get(symbol)
        if idx is None: return -1

        features = self.loader.feat_tensor[idx] # 2D Tensor
        features_batch = features.unsqueeze(0)  # [1, F, T]
        
        res = self.vm.execute(self.formula, features_batch)
        if res is None: return -1
        
        latest_logit = res[0, -1]
        score = torch.sigmoid(latest_logit).item()
        return score

    async def shutdown(self):
        logger.info("O.o | 正在关闭交易所连接并安全退出...")
        await self.loader.close()
        await self.trader.close()


async def main():
    runner = OKXConfiguredRunner()
    try:
        await runner.initialize()
        await runner.run_loop()
    except asyncio.CancelledError:
        pass
    finally:
        await runner.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("用户手动中断运行 (KeyboardInterrupt)，程序正在安全退出。")