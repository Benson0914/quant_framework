import logging
from datetime import datetime
from bson.objectid import ObjectId
from db_schema import MongoDBManager
from Config_q import Config
import pandas as pd
import logging

class PositionManager:
    def __init__(self):
        self.db = MongoDBManager()
        self.slippage = Config.SLIPPAGE
        self.fee_rate = Config.FEE_RATE
        self.risk_per_trade = Config.RISK_PER_TRADE
        self.stop_loss_atr = Config.STOP_LOSS_ATR
        self.take_profit_atr = Config.TAKE_PROFIT_ATR
        self.leverage = Config.LEVERAGE
        self.symbol_precision = Config.SYMBOL_PRECISION
        self.capital = Config.CAPITAL
        self.max_positions = Config.MAX_POSITIONS
        self.max_risk = Config.MAX_RISK

    def calculate_position_size(self, symbol, entry_price, atr, capital):
        try:
            risk_amount = capital * self.risk_per_trade
            stop_loss_distance = atr * self.stop_loss_atr
            qty = risk_amount / (stop_loss_distance * self.leverage)
            qty = round(qty, self.symbol_precision)
            logging.info(f"Calculated position size for {symbol}: {qty} at entry_price {entry_price}")
            return qty
        except Exception as e:
            logging.error(f"Calculate position size failed for {symbol}: {e}")
            return 0

    def get_open_positions(self):
        """ 從 MongoDB 抓取所有未平倉（status='open')部位 """
        try:
            positions = list(self.db.db.positions.find({"status": "open"}))
            return positions
        except Exception as e:
            logging.error(f"Failed to fetch open positions: {e}")
            return []

    def check_position_limit(self, symbol, side, entry_price, atr):
        try:
            positions = self.get_open_positions()
            capital = self.capital
            qty = self.calculate_position_size(symbol, entry_price, atr, capital)
            if qty <= 0:
                logging.warning(f"Invalid position size for {symbol}: {qty}")
                return None

            if len(positions) >= self.max_positions:
                logging.warning(f"Cannot open position for {symbol}: Max positions reached")
                return None

            total_risk = 0
            for p in positions:
                p_entry = p['entry_price']
                p_stop = p['stop_loss']
                p_qty = p['qty']
                total_risk += abs(p_entry - p_stop) * p_qty

            if side == 'long':
                stop_loss_price = entry_price - atr * self.stop_loss_atr
            else:
                stop_loss_price = entry_price + atr * self.stop_loss_atr

            new_risk = abs(entry_price - stop_loss_price) * qty
            if total_risk + new_risk > self.max_risk * capital:
                logging.warning(f"Cannot open position for {symbol}: total risk exceeds {self.max_risk * 100}%")
                return None

            return qty

        except Exception as e:
            logging.error(f"check_position_limit failed for {symbol}: {e}")
            return None

    def has_open_position(self, symbol: str, side: str, entry_price) -> bool:
        try:
            query = {
                "symbol": symbol,
                "side": side,
                "entry_price": float(entry_price),
                "status": "open"
            }
            pos = self.db.db.positions.find_one(query)
            return pos is not None
        except Exception as e:
            logging.error(f"has_open_position check failed: {e}")
            return False

    def open_position(self, symbol, side, entry_price, entry_time, qty, atr, stop_loss, take_profit):
        try:
            position_doc = {
                "symbol": symbol,
                "side": side,
                "qty": float(qty),
                "entry_price": float(entry_price),
                "atr": float(atr),
                "stop_loss": float(stop_loss) if stop_loss is not None else 0.0,
                "take_profit": float(take_profit) if take_profit is not None else 0.0,
                "open_time": entry_time.isoformat() if hasattr(entry_time, 'isoformat') else str(entry_time),
                "status": "open"
            }
            result = self.db.db.positions.insert_one(position_doc)
            position_doc['_id'] = result.inserted_id
            logging.info(f"Position opened successfully: {position_doc}")
            return position_doc
        except Exception as e:
            logging.error(f"Failed to open position: {e}")
            return None
        
    def check_exit_conditions(self, position: dict, df: pd.DataFrame):
        """
        判斷持倉是否符合平倉條件（止損、停利）

        參數：
        - position: dict，MongoDB 查出的持倉文件
            須包含 key: 'side' (str), 'stop_loss' (float), 'take_profit' (float), 'open_time' (ISO 字串)
        - df: pandas.DataFrame，行情資料，需要包含
            ['timestamp', 'open', 'high', 'low', 'close']欄位，timestamp為datetime格式

        回傳：
        - exited (bool): 是否觸發平倉條件
        - exit_price (float or None): 平倉價，如果沒觸發為 None
        - exit_time (datetime or None): 平倉時間
        - reason (str or None): 平倉原因 'stop_loss' 或 'take_profit'，未觸發為 None
        """
        try:
            # 確保df['timestamp']長度並轉為datetime
            if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                df['timestamp'] = pd.to_datetime(df['timestamp'])

            # 取得持倉開倉時間轉換成Timestamp
            entry_time = pd.to_datetime(position.get('open_time') or position.get('entry_time'))
            # 篩選從開倉時間開始以後的行情數據索引
            idx_labels = df[df['timestamp'] >= entry_time].index
            idx_positions = df.index.get_indexer(idx_labels)

            for j in idx_positions:
                if j < 0 or j >= len(df):
                    logging.warning(f"Index {j} out of range for DataFrame of length {len(df)}")
                    continue

                bar = df.iloc[j]
                stop = position.get('stop_loss')
                take = position.get('take_profit')

                # 如果 stop_loss 或 take_profit 缺失，跳過判斷
                if stop is None or take is None:
                    continue

                side = position.get('side', '').lower()

                if side == 'long':
                    # 多單止損條件
                    if bar['low'] <= stop:
                        exit_price = stop * (1 - self.slippage)
                        reason = 'stop_loss'
                        return True, exit_price, bar['timestamp'], reason
                    # 多單停利條件
                    elif bar['high'] >= take:
                        exit_price = take * (1 - self.slippage)
                        reason = 'take_profit'
                        return True, exit_price, bar['timestamp'], reason

                elif side == 'short':
                    # 空單止損條件
                    if bar['high'] >= stop:
                        exit_price = max(bar['open'], stop) * (1 + self.slippage)
                        reason = 'stop_loss'
                        return True, exit_price, bar['timestamp'], reason
                    # 空單停利條件
                    elif bar['low'] <= take:
                        exit_price = min(bar['open'], take) * (1 + self.slippage)
                        reason = 'take_profit'
                        return True, exit_price, bar['timestamp'], reason

            # 沒有觸發任何平倉條件
            return False, None, None, None

        except Exception as e:
            logging.error(f"Check exit conditions failed for {position.get('symbol', 'unknown')}: {e}")
            return False, None, None, None


    def close_position(self, position_id, exit_price, exit_time, reason):
        try:
            pos_obj_id = position_id if isinstance(position_id, ObjectId) else ObjectId(position_id)
            position = self.db.db.positions.find_one({"_id": pos_obj_id})
            if position is None:
                logging.error(f"Position id {position_id} not found")
                return None

            side = position['side']
            entry_price = position['entry_price']
            qty = position['qty']

            gross_pnl = (exit_price - entry_price) * qty if side == 'long' else (entry_price - exit_price) * qty
            fee = (abs(entry_price) + abs(exit_price)) * qty * self.fee_rate / self.leverage
            pnl = gross_pnl - fee

            # 更新持倉狀態
            self.db.db.positions.update_one(
                {"_id": pos_obj_id},
                {"$set": {"status": "closed"}}
            )

            trade_doc = {
                "symbol": position['symbol'],
                "side": side,
                "qty": qty,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "open_time": position['open_time'],
                "close_time": exit_time.isoformat() if hasattr(exit_time, 'isoformat') else str(exit_time),
                "gross_pnl": gross_pnl,
                "fee": fee,
                "pnl": pnl,
                "reason": reason
            }
            self.db.db.trades.insert_one(trade_doc)

            logging.info(f"Position closed successfully: {trade_doc}")
            return trade_doc
        except Exception as e:
            logging.error(f"Close position failed: {e}")
            return None

    def close(self):
        self.db.close()
