import pandas as pd
import logging
from datetime import datetime
from pymongo import ASCENDING
from db_schema import MongoDBManager 
from strategy import IndicatorCalculator, SignalGenerator
from position_manager import PositionManager
from performace_check import PerformanceAnalyzer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class BacktestEngine:
    def __init__(self):
        self.db = MongoDBManager()
        self.ic = IndicatorCalculator()  # Calcalation
        self.sg = SignalGenerator()      # Signals from MongoDB read and write
        self.pm = PositionManager()      # Position Management from MongoDB
        
        self.symbols = self.ic.symbols
        self.timeframe = '1h'
        self.all_trades_history = []

        logging.info("BacktestEngine initialized")
        logging.info(f"Symbols: {self.symbols}")
        logging.info(f"Timeframe: {self.timeframe}")

    def run(self):
        logging.info("Starting Backtest")

        for symbol in self.symbols:
            logging.info(f"\n--- Backtesting {symbol} ---")

            # Reset open positions and trades
            # self.pm.reset_states()

            # 1. From MongoDB get OHLCV (DataFrame)
            df = self.ic.fetch_ohlcv(symbol, self.timeframe, limit=1000)
            if df.empty:
                logging.warning(f"No OHLCV data for {symbol}. Skipping.")
                continue
            df['timestamp'] = pd.to_datetime(df['timestamp'])

            # 2. Indicators Calculate
            df_h1_indicators, df_d1_indicators = self.ic.calculate_indicators(symbol)
            if df_h1_indicators.empty or df_d1_indicators.empty:
                logging.warning(f"Failed to calculate indicators for {symbol}. Skipping.")
                continue

            df_h1_indicators['timestamp'] = pd.to_datetime(df_h1_indicators['timestamp'])
            df_processed = pd.merge(df, df_h1_indicators[
                ['timestamp', 'donchian_high_20', 'donchian_low_20', 'atr_14']
            ], on='timestamp', how='left').dropna(subset=['atr_14'])

            if df_processed.empty:
                logging.warning(f"No processed OHLCV data after merge for {symbol}. Skipping.")
                continue

            # 3. From MongoDB signals collection get singals
            signals_cursor = self.sg.db.db.signals.find({"symbol": symbol, "timeframe": self.timeframe}).sort("timestamp", ASCENDING)
            signals_data = list(signals_cursor)
            if not signals_data:
                logging.info(f"No signals for {symbol}. Skipping.")
                continue

            signals_df = pd.DataFrame(signals_data)
            signals_df['timestamp'] = pd.to_datetime(signals_df['timestamp'])

            # 4. backtesting：
            signals_iter = iter(signals_df.itertuples())
            current_signal = next(signals_iter, None)

            for idx, bar in df_processed.iterrows():
                current_time = bar['timestamp']

                # Check exit conditions
                open_positions = self.pm.get_open_positions()
                for pos in open_positions:
                    data_since_entry = df_processed[df_processed['timestamp'] >= pd.to_datetime(pos['open_time'])]
                    exited, exit_price, exit_time, reason = self.pm.check_exit_conditions(pos, data_since_entry)
                    if exited:
                        trade = self.pm.close_position(pos['_id'], exit_price, exit_time, reason)
                        if trade is not None:
                            logging.info(f"Closed position ID {pos['_id']} - PnL: {trade['pnl']:.2f}")
                        else:
                            logging.error(f"Failed to close position ID {pos['_id']}")


                # check signals time <= now bar
                while current_signal and current_signal.timestamp <= current_time:
                    signal_type = current_signal.signal_type
                    entry_price = current_signal.value
                    stop_loss = current_signal.stop_loss
                    take_profit = current_signal.take_profit

                    atr_val = bar['atr_14']
                    if pd.isna(atr_val):
                        logging.warning(f"ATR NaN at {current_time} for {symbol}. Skip signal.")
                        current_signal = next(signals_iter, None)
                        continue

                    qty = self.pm.check_position_limit(symbol, signal_type, entry_price, atr_val)
                    if qty and qty > 0:
                        if self.pm.has_open_position(symbol, signal_type, entry_price):
                            logging.info(f"Already have open position for {symbol} {signal_type} at {entry_price}.")
                        else:
                            pos = self.pm.open_position(symbol, signal_type, entry_price, current_time, qty, atr_val, stop_loss, take_profit)
                            if pos:
                                logging.info(f"Opened position ID: {pos['_id']}")
                            else:
                                logging.warning(f"Failed to open position for {symbol} at {current_time}")
                    else:
                        logging.info(f"Position limit prevents opening {symbol} {signal_type} at {current_time}.")

                    current_signal = next(signals_iter, None)

            # 回測結束，強制平倉所有剩餘持倉
            logging.info(f"End of backtest for {symbol}. Closing remaining positions.")
            open_positions = self.pm.get_open_positions()
            if len(open_positions) > 0:
                last_bar = df_processed.iloc[-1]
                for pos in open_positions:
                    trade = self.pm.close_position(pos['_id'], last_bar['close'], last_bar['timestamp'], "End of Backtest")
                    logging.info(f"Forced close position ID {pos['_id']} - PnL: {trade['pnl']:.2f}")

            # Performance analysis
            trades_cursor = self.pm.db.db.trades.find({})
            trades_list = list(trades_cursor)
            if trades_list:
                pa = PerformanceAnalyzer(trades_list)
                summary = pa.summary()
                for k, v in summary.items():
                    logging.info(f"{k}: {v}")
                pa.plot_equity(symbol=symbol)
                # self.all_trades_history.extend(self.pm.trades)
            else:
                logging.info(f"No trades recorded for {symbol}.")

        # Return the overall performance
        logging.info("All backtests complete.")
        if self.all_trades_history:
            overall_pa = PerformanceAnalyzer(self.all_trades_history)
            overall_summary = overall_pa.summary()
            for k, v in overall_summary.items():
                logging.info(f"{k}: {v}")
            overall_pa.plot_equity(symbol="Overall Portfolio")
        else:
            logging.info("No trades recorded overall.")

    def close(self):
        self.db.close()
        self.ic.db.close()
        self.sg.db.close()
        self.pm.close()


if __name__ == '__main__':
    engine = BacktestEngine()
    engine.run()
    engine.close()
