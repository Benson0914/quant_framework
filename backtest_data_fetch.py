import ccxt.async_support as ccxt_async
import asyncio
import pandas as pd
from Config_q import Config
from db_schema import MongoDBManager
import logging

START_DATE = '2022-01-01T00:00:00'
END_DATE = '2025-01-01T00:00:00'
OHLCV_LIMIT = 1000

class HistoricalFetcher:
    def __init__(self):
        self.db = MongoDBManager()
        self.symbols = Config.SYMBOLS
        self.timeframes = Config.TIMEFRAMES
        self.exchange = ccxt_async.binance({
            'enableRateLimit': True,
            'options': {'defaultType': 'future'},
        })

    # ... fetch_ohlcv_range ...
    async def fetch_ohlcv_range(self, symbol, timeframe, since, until):
        all_data = []
        while since < until:
            try:
                ohlcv = await self.exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=OHLCV_LIMIT)
                if not ohlcv:
                    break
                # 過濾超出 until 的資料
                ohlcv = [c for c in ohlcv if c[0] < until]
                if not ohlcv:
                    break
                all_data.extend(ohlcv)
                since = ohlcv[-1][0] + 1
                if len(ohlcv) < OHLCV_LIMIT:
                    break
            except Exception as e:
                logging.error(f"Fetch error {symbol} {timeframe}: {e}")
                await asyncio.sleep(2)
        return all_data
    
    async def fetch_and_store(self, symbol, timeframe):
        print(f"Start fetching {symbol} {timeframe}")
        since = int(pd.Timestamp(START_DATE).timestamp() * 1000)
        until = int(pd.Timestamp(END_DATE).timestamp() * 1000)
        ohlcv = await self.fetch_ohlcv_range(symbol, timeframe, since, until)
        if not ohlcv:
            print(f"No data for {symbol} {timeframe}")
            return 
        columns = ["timestamp", "open", "high", "low", "close", "volume"]
        data = pd.DataFrame(ohlcv, columns=columns)
        data['symbol'] = symbol
        data['timeframe'] = timeframe
        data['exchange'] = 'binance'

        def safe_iso(x):
            try:
                if pd.isna(x):
                    return None
                return pd.Timestamp(int(x), unit='ms').isoformat()
            except Exception:
                return None

        data['timestamp'] = data['timestamp'].apply(safe_iso)
        data = data.dropna(subset=['timestamp'])

        for _, row in data.iterrows():
            doc = {
                "symbol": row['symbol'],
                "timeframe": row['timeframe'],
                "timestamp": row['timestamp'],
                "open": row['open'],
                "high": row['high'],
                "low": row['low'],
                "close": row['close'],
                "volume": row['volume'],
                "exchange": row['exchange']
            }
            self.db.insert_ohlcv(doc)

        print(f"Stored {len(data)} rows for {symbol} {timeframe}")

    async def fetch_all(self):
        tasks = []
        for symbol in self.symbols:
            for timeframe in self.timeframes:
                tasks.append(self.fetch_and_store(symbol, timeframe))
        await asyncio.gather(*tasks)

    async def close(self):
        await self.exchange.close()
        self.db.close()
        
if __name__ == "__main__":
    async def main():
        fetcher = HistoricalFetcher()
        await fetcher.fetch_all()
        await fetcher.close()
    asyncio.run(main())
