import asyncio
import pandas as pd
from loguru import logger

from data_pipeline.providers.okx import OKXProvider
from data_pipeline.db_manager import DBManager
from dotenv import load_dotenv

load_dotenv()

async def run_okx_training_pipeline(limit=50, export_csv_path="okx_processed_training_data.csv"):
    logger.info("启动 OKX 专属训练数据管线 ...")
    
    okx = OKXProvider()
    db = DBManager()
    
    # # 1. 初始化并连接数据库
    await db.connect()
    await db.init_schema()  # 确保表结构和 TimescaleDB 超表已创建

    try:
        logger.info(f"[获取层] 正在从 OKX 获取 Top {limit} 交易对...")
        tokens = await okx.get_trending_tokens(limit=limit)
        
        if not tokens:
            logger.error("未获取到代币列表，程序退出。")
            return

        # 2. 将基础代币信息写入 tokens 表
        # tokens 列表内字典包含: address, symbol, name, decimals, liquidity, fdv
        # upsert_tokens 接受格式: (address, symbol, name, decimals, chain)
        token_records = [
            (t['address'], t['symbol'], t['name'], t['decimals'], 'OKX') 
            for t in tokens
        ]
        await db.upsert_tokens(token_records)
        logger.success(f"成功将 {len(token_records)} 个交易对基础信息同步至 tokens 表")

        # 3. 遍历获取历史 K 线并直接入库
        for token in tokens:
            address = token['address']
            logger.info(f"[获取层] 拉取 {address} 历史 K 线数据...")
            
            history_tuples = await okx.get_token_history(
                session=None, 
                address=address,
                liquidity=token.get('liquidity'),
                fdv=token.get('fdv')
            )
            
            if history_tuples:
                # 直接调用 batch_insert_ohlcv 即可，底层使用了 asyncpg 的 copy_records_to_table，性能极高
                logger.info(history_tuples)
                await db.batch_insert_ohlcv(history_tuples)
                logger.success(f"[存储层] 成功为 {address} 写入 {len(history_tuples)} 条 K 线数据")
            
            # OKXProvider 内部虽然有 semaphore，但稍微加个 sleep 让输出更平滑
            await asyncio.sleep(0.5)

        # 4. 从数据库中导出数据
        logger.info("[导出层] 正在从 PostgreSQL 提取数据用于训练...")
        async with db.pool.acquire() as conn:
            # 使用 asyncpg 原生查询
            query = """
                SELECT time, address, open, high, low, close, volume, liquidity, fdv, source 
                FROM ohlcv 
                ORDER BY address ASC, time ASC
            """
            records = await conn.fetch(query)
            
            if records:
                # 将 asyncpg 的 Record 对象转换为 pandas DataFrame
                df = pd.DataFrame([dict(record) for record in records])
                df.to_csv(export_csv_path, index=False)
                logger.info(f"🎉 完美收工！共导出 {len(df)} 条训练数据至: {export_csv_path}")
            else:
                logger.warning("数据库中未查到 OKX 的 ohlcv 数据。")

    except Exception as e:
        logger.error(f"管线执行期间发生错误: {e}")

    finally:
        # 5. 优雅关闭资源
        await db.close()
        await okx.close()

if __name__ == "__main__":
    
    asyncio.run(run_okx_training_pipeline(limit=50))