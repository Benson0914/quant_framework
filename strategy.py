# 請先安裝 pandas_ta: pip install pandas_ta
import pandas as pd
import talib
import logging
from pymongo import ASCENDING
from bson.objectid import ObjectId
from db_schema import MongoDBManager
from Config_q import Config

class IndicatorCalculator:
    def __init__(self):
        self.db = MongoDBManager()
        self.symbols = Config.SYMBOLS
        self.timeframes = Config.TIMEFRAMES
        self.stop_loss_atr = Config.STOP_LOSS_ATR
        self.take_profit_atr = Config.TAKE_PROFIT_ATR

# get OHLCV
    def fetch_ohlcv(self, symbol, timeframe='1h', limit=500):
        try:
            cursor = self.db.db.ohlcv.find(
                {"symbol": symbol, "timeframe": timeframe}
            ).sort("timestamp", ASCENDING).limit(limit)
            
            data = list(cursor)
            if not data:
                return pd.DataFrame()
            
            
            df = pd.DataFrame(data)

            if 'timestamp' in df.columns:
                # if timestamp = ISO type, transfer to datetime
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            return df
        except Exception as e:
            logging.error(f"Failed to fetch OHLCV from MongoDB for {symbol} {timeframe}: {e}")
            return pd.DataFrame()    
        
    def calculate_indicators(self, symbol):
            try:
                df_h1 = self.fetch_ohlcv(symbol, '1h', 1000)
                if df_h1.empty or len(df_h1) < 50:
                    return pd.DataFrame(), pd.DataFrame()

                df_h1['donchian_high_20'] = talib.MAX(df_h1['high'], timeperiod=20)
                df_h1['donchian_low_20'] = talib.MIN(df_h1['low'], timeperiod=20)
                df_h1['donchian_high_50'] = talib.MAX(df_h1['high'], timeperiod=50)
                df_h1['donchian_low_50'] = talib.MIN(df_h1['low'], timeperiod=50)
                df_h1['atr_14'] = talib.ATR(df_h1['high'], df_h1['low'], df_h1['close'], timeperiod=14)

                df_d1 = self.fetch_ohlcv(symbol, '1d', 30)
                if df_d1.empty:
                    return df_h1, pd.DataFrame()
                
                df_d1['momentum_14'] = talib.MOM(df_d1['close'], timeperiod=14)
                return df_h1, df_d1
            except Exception as e:
                logging.error(f"Indicator calculation failed for {symbol}: {e}")
                return pd.DataFrame(), pd.DataFrame()

    def close(self):
        self.db.close()

class SignalGenerator:
    def __init__(self):
        self.db = MongoDBManager()
        self.indicator = IndicatorCalculator()

    def generate_signals(self, symbol):
        try:
            df_h1, df_d1 = self.indicator.calculate_indicators(symbol)
            if df_h1.empty or df_d1.empty:
                logging.warning(f"No data to generate signals for {symbol}")
                return

            signals_to_insert = []
            
            for i in range(50, len(df_h1)):
                h1 = df_h1.iloc[i]
                d1 = df_d1.iloc[-1]  # 最新日線資料

                long_signal = (h1['close'] >= h1['donchian_high_20'] * 0.995 and d1['momentum_14'] > 0)
                short_signal = (h1['close'] <= h1['donchian_low_20'] * 1.005 and d1['momentum_14'] < 0)

                if long_signal or short_signal:
                    signal_type = 'long' if long_signal else 'short'

                    stop_loss = (h1['close'] - self.indicator.stop_loss_atr * h1['atr_14'] 
                                 if long_signal else h1['close'] + self.indicator.stop_loss_atr * h1['atr_14'])
                    take_profit = (h1['close'] + self.indicator.take_profit_atr * h1['atr_14'] 
                                   if long_signal else h1['close'] - self.indicator.take_profit_atr * h1['atr_14'])
                    
                    signal_doc = {
                        "symbol": symbol,
                        "timeframe": "1h",
                        "signal_type": signal_type,
                        "value": float(h1['close']),
                        "timestamp": h1['timestamp'].isoformat() if not pd.isna(h1['timestamp']) else None,
                        "stop_loss": float(stop_loss),
                        "take_profit": float(take_profit),
                        "processed": 0
                    }
                    signals_to_insert.append(signal_doc)
            
            if signals_to_insert:
                # 批量插入
                self.db.db.signals.insert_many(signals_to_insert)
                logging.info(f"{len(signals_to_insert)} signals stored for {symbol}")
        except Exception as e:
            logging.error(f"Signal generation failed for {symbol}: {e}")

    def close(self):
        self.db.close()
        self.indicator.close()

if __name__ == '__main__':
    ic = IndicatorCalculator()
    sg = SignalGenerator()
    for symbol in ic.symbols:
        sg.generate_signals(symbol) 
    sg.close()
