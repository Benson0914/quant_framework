import motor.motor_asyncio
from pymongo import UpdateOne
from datetime import datetime
import pandas as pd
import logging
import asyncio
from retrying import retry
from Config_q import Config
import ccxt.async_support as ccxt_async
import os


class DataFetcher:
    def __init__(self, mongo_uri='mongodb://localhost:27017', db_name='quant_trading'):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)
        self.db = self.client[db_name]
        self.config = Config() #call the Config
        self.exchange = ccxt_async.binance({ #API connection
            'enableRateLimit': True,
            'options': {'defaultType': 'future'},
            'apiKey': os.getenv('BINANCE_API_KEY', ''),
            'secret': os.getenv('BINANCE_API_SECRET', '')
        })
        self.symbols = Config.SYMBOLS
        self.timeframes = Config.TIMEFRAMES
        self.ohlcv_cache = {}
        self.orderbook_cache = {}

    @retry(stop_max_attempt_number=3, wait_fixed=5000) #retry
    async def fetch_and_store_ohlcv(self, symbol: str, timeframe: str, limit: int = Config.OHLCV_LIMIT) -> pd.DataFrame:
        try:
            symbol_api = f"{symbol}:USDT" if '/' in symbol else symbol
            cache_key = f"binance_{symbol}_{timeframe}"

            # 取得該symbol和timeframe的最大timestamp
            last_doc = await self.db.ohlcv.find_one(
                {'symbol': symbol, 'timeframe': timeframe, 'exchange': 'binance'},
                sort=[('timestamp', -1)]
            )
            since = None
            if last_doc:
                # MongoDB stores ISODate
                since = int(last_doc['timestamp'].timestamp() * 1000) + 1

            ohlcv = await self.exchange.fetch_ohlcv(symbol_api, timeframe, since=since, limit=limit)
            logging.info(f"Fetched {len(ohlcv)} OHLCV rows for {symbol} {timeframe}")
            
            if not ohlcv:
                logging.warning(f"No new OHLCV data for {symbol} {timeframe}")
                return pd.DataFrame()

            data = []
            for row in ohlcv:
                data.append({
                    'timestamp': datetime.utcfromtimestamp(row[0] / 1000),
                    'open': row[1],
                    'high': row[2],
                    'low': row[3],
                    'close': row[4],
                    'volume': row[5],
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'exchange': 'binance'
                })

            # upsert or ignore duplicate key error
            operations = []
            for doc in data:
                filter = {'symbol': doc['symbol'], 'timeframe': doc['timeframe'], 'exchange': doc['exchange'], 'timestamp': doc['timestamp']}
                operations.append(UpdateOne(filter, {'$set': doc}, upsert=True))

            if operations:
                result = await self.db.ohlcv.bulk_write(operations)
                logging.info(f"MongoDB upserted {result.upserted_count + result.modified_count} OHLCV documents for {symbol} {timeframe}")
            
            # update cache
            df = pd.DataFrame(ohlcv, columns=['time_ms', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['time_ms'], unit='ms')
            df.drop(columns=['time_ms'], inplace=True)
            df['symbol'] = symbol
            df['timeframe'] = timeframe
            df['exchange'] = 'binance'
            self.ohlcv_cache[cache_key] = df

            return df
        except Exception as e:
            logging.error(f"OHLCV fetch failed for binance {symbol} {timeframe}: {str(e)}")
            raise

    @retry(stop_max_attempt_number=3, wait_fixed=5000)
    async def fetch_and_store_orderbook(self, symbol: str) -> dict:
        try:
            symbol_api = f"{symbol}:USDT" if '/' in symbol else symbol
            cache_key = f"binance_{symbol}"

            orderbook = await self.exchange.fetch_order_book(symbol_api, limit=self.config.ORDERBOOK_LIMIT)
            logging.info(f"Fetched orderbook for {symbol}")

            data = {
                'timestamp': datetime.utcnow(),
                'symbol': symbol,
                'exchange': 'binance',
                'bid_price': orderbook['bids'][0][0] if orderbook['bids'] else 0,
                'bid_qty': sum(bid[1] for bid in orderbook['bids']),
                'ask_price': orderbook['asks'][0][0] if orderbook['asks'] else 0,
                'ask_qty': sum(ask[1] for ask in orderbook['asks'])
            }
            
            # upsert orderbook by symbol and exchange - 如果想儲存歷史可用 insert_one
            await self.db.orderbook.update_one(
                {'symbol': data['symbol'], 'exchange': data['exchange']},
                {'$set': data},
                upsert=True
            )
            self.orderbook_cache[cache_key] = data
            logging.info(f"Stored orderbook for {symbol} on binance into MongoDB")
            return data
        except Exception as e:
            logging.error(f"Orderbook fetch failed for binance {symbol}: {str(e)}")
            raise

    async def fetch_all_ohlcv(self) -> None:
        tasks = []
        for symbol in self.symbols:
            for timeframe in self.timeframes:
                tasks.append(self.fetch_and_store_ohlcv(symbol, timeframe))
        await asyncio.gather(*tasks, return_exceptions=True)
        logging.info("Completed OHLCV fetch for all symbols and timeframes")

    async def fetch_all_orderbook(self) -> None:
        tasks = []
        for symbol in self.symbols:
            tasks.append(self.fetch_and_store_orderbook(symbol))
        await asyncio.gather(*tasks, return_exceptions=True)
        logging.info("Completed orderbook fetch for all symbols")

    async def loop_ohlcv(self) -> None:
        while True:
            try:
                await self.fetch_all_ohlcv()
                await asyncio.sleep(self.config.OHLCV_UPDATE_INTERVAL)
            except Exception as e:
                logging.error(f"OHLCV fetch loop error: {str(e)}")
                await asyncio.sleep(300)

    async def loop_orderbook(self) -> None:
        while True:
            try:
                await self.fetch_all_orderbook()
                await asyncio.sleep(self.config.ORDERBOOK_UPDATE_INTERVAL)
            except Exception as e:
                logging.error(f"Orderbook fetch loop error: {str(e)}")
                await asyncio.sleep(300)

    async def run(self) -> None:
        await asyncio.gather(self.loop_ohlcv(), self.loop_orderbook())

    async def close(self) -> None:
        await self.exchange.close()
        self.client.close()
        logging.info("MongoDB and Binance connections closed")
