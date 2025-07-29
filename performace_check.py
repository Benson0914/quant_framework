import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

class PerformanceAnalyzer:
    def __init__(self, trades):
        
        self.trades = pd.DataFrame(trades)
        if 'exit_time' in self.trades.columns:
            self.trades = self.trades.sort_values('exit_time')
        self.equity_curve = self._calc_equity_curve()
        self.x_axis = range(1, len(self.equity_curve) + 1)
    
    def _calc_equity_curve(self):
        eq = self.trades['pnl'].cumsum().fillna(0)
        return eq
    
    def summary(self):
        total_pnl = self.trades['pnl'].sum()
        returns = self.trades['pnl'].values
        win_trades = self.trades[self.trades['pnl'] > 0]
        loss_trades = self.trades[self.trades['pnl'] < 0]
        win_rate = len(win_trades) / len(self.trades) if len(self.trades) > 0 else np.nan
        profit_factor = win_trades['pnl'].sum() / abs(loss_trades['pnl'].sum()) if len(loss_trades) > 0 else np.nan
        # Max Drawdown
        equity = self.equity_curve.values
        peak = np.maximum.accumulate(equity)
        drawdown = (peak - equity)
        max_drawdown = drawdown.max() if len(drawdown) > 0 else np.nan
        # Sharpe Ratio
        sharpe = np.mean(returns) / (np.std(returns) + 1e-8) * np.sqrt(252) if len(returns) > 1 else np.nan
        # Avg win/loss
        avg_win = win_trades['pnl'].mean() if len(win_trades) > 0 else 0
        avg_loss = loss_trades['pnl'].mean() if len(loss_trades) > 0 else 0

        return {
            'Total PnL': round(total_pnl, 2),
            'Max Drawdown': round(max_drawdown, 2),
            'Win Rate': f"{win_rate*100:.2f}%" if not np.isnan(win_rate) else 'N/A',
            'Sharpe': round(sharpe, 2) if not np.isnan(sharpe) else 'N/A',
            'Profit Factor': round(profit_factor, 2) if not np.isnan(profit_factor) else 'N/A',
            'Avg Win': round(avg_win, 2),
            'Avg Loss': round(avg_loss, 2),
            'Total Trades': len(self.trades)
        }
    
    def plot_equity(self, symbol=None):
        plt.figure(figsize=(10, 4))
        plt.plot(self.x_axis, self.equity_curve, label='Equity Curve')
        plt.title(f'Equity Curve' + (f' ({symbol})' if symbol else ''))
        plt.xlabel('Trade #')
        plt.ylabel('Equity')
        plt.grid(True)
        plt.legend()
        plt.tight_layout()
        plt.show()
