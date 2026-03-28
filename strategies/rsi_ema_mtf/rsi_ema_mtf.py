"""
Example for a simple Multi-Timeframe
Multi-Indicator Strategy
"""

from collections import defaultdict
from typing import Optional, List, Union
import polars as pl # type: ignore

from backtester.system.strategy import BaseStrategy, Signal
from backtester.system.interfaces import Position, IPortfolio


class RSI_EMA_MultiTimeframe(BaseStrategy):
    """
    Multi-timeframe strategy using RSI and EMA indicators.

    Logic:
    - 15m EMA_20: Primary trend filter (price above = bullish bias, below = bearish bias)
    - 5m RSI_14: Entry setup detector (oversold/overbought in trend direction)
    - 1m EMA_10: Precise entry trigger (price crosses EMA in setup direction)

    Entry Conditions:
    - LONG: 15m bullish trend + 5m RSI oversold (<40) + 1m price crosses above EMA_10
    - SHORT: 15m bearish trend + 5m RSI overbought (>60) + 1m price crosses below EMA_10

    Exit Conditions:
    - Stop loss / Take profit (handled explicitly in on_trigger)
    - Trend reversal on 15m (price crosses EMA_20 against position)
    - Losing streak detector (stops new entries if last 3 trades were losers)
    """

    def __init__(self, strategy_id: str, config: dict):
        super().__init__(strategy_id, config)

        # QD explicitly defines risk parameters here, not in YAML.
        self.stop_loss_pct = 0.02
        self.take_profit_pct = 0.03

        # State tracking per symbol
        self.state = defaultdict(lambda: {
            "trend_15m": "NEUTRAL",           # BULLISH, BEARISH, NEUTRAL
            "rsi_setup_5m": "NONE",           # OVERSOLD, OVERBOUGHT, NONE
            "prev_price_1m": None,            # For detecting EMA crosses
            "entry_ready": True               # Default to True, disable on losing streak
        })

    @staticmethod
    def get_data_transform(df: pl.DataFrame, timeframe: str) -> pl.DataFrame:
        """
        Defines the data requirements for this strategy.
        Injected into the DataProvider to calculate indicators on the fly.
        """
        # Helper: EMA Calculation
        def calculate_ema(df: pl.DataFrame, period: int, column: str = "c") -> pl.DataFrame:
            alpha = 2 / (period + 1)
            ema_col_name = f"EMA_{period}"
            return df.with_columns([pl.col(column).ewm_mean(alpha=alpha, adjust=False).alias(ema_col_name)])

        # Helper: RSI Calculation
        def calculate_rsi(df: pl.DataFrame, period: int = 14) -> pl.DataFrame:
            price_change = pl.col("c") - pl.col("c").shift(1)
            gain = pl.when(price_change > 0).then(price_change).otherwise(0)
            loss = pl.when(price_change < 0).then(-price_change).otherwise(0)
            alpha = 1 / period
            avg_gain = gain.ewm_mean(alpha=alpha, adjust=False)
            avg_loss = loss.ewm_mean(alpha=alpha, adjust=False)
            rs = avg_gain / avg_loss
            rsi_val = 100 - (100 / (1 + rs))
            return df.with_columns(rsi_val.alias(f"RSI_{period}"))

        if timeframe == '5m':
            # Calculate RSI_14
            df = calculate_rsi(df, period=14)

        elif timeframe == '15m':
            # Calculate EMA_20
            df = calculate_ema(df, period=20, column="c")

        elif timeframe == '1m':
            # Calculate EMA_10
            df = calculate_ema(df, period=10, column="c")

        return df

    def on_trigger(self, data: dict, execution_data: dict) -> Optional[Union[Signal, List[Signal]]]:
        """
        Process incoming market data and generate signals.
        All entry and exit logic is contained within this method.
        """
        symbol = data['symbol']
        price = data['c']
        ts = data['timestamp']
        state = self.state[symbol]

        # Fetch open trades manually
        open_trades = self.portfolio.get_active_positions(self.strategy_id, symbol)

        # --- 1. Prioritize Exit Logic for Open Trades ---
        if open_trades:
            exit_signals = []
            for trade in open_trades:
                # Stop Loss Check
                if trade.quantity > 0 and price <= trade.entry_price * (1 - self.stop_loss_pct):
                    exit_signals.append(self.create_signal(symbol, "EXIT", 0, ts, "STOP_LOSS", price, trade_id_to_close=trade.trade_id))

                # Take Profit Check
                elif trade.quantity > 0 and price >= trade.entry_price * (1 + self.take_profit_pct):
                    exit_signals.append(self.create_signal(symbol, "EXIT", 0, ts, "TAKE_PROFIT", price, trade_id_to_close=trade.trade_id))

            if exit_signals:
                return exit_signals # Immediately exit if any SL/TP is hit

        # --- 2. State Management & Entry Logic ---
        tf = data['timeframe']
        if tf == "15m":
            ema_20 = data.get('EMA_20')
            if ema_20 is None: return None

            new_trend = "BULLISH" if price > ema_20 else "BEARISH"
            old_trend = state.get('trend_15m')

            # Trend reversal exit logic
            if new_trend != old_trend:
                # Log trend change at strategy level (no symbol parameter)
                self.log(f"Trend Change for {symbol}: {old_trend} -> {new_trend} (Price: {price:.2f}, EMA: {ema_20:.2f})")

                if open_trades:
                    for trade in open_trades:
                        if trade.quantity > 0 and new_trend == "BEARISH":
                            return self.create_signal(symbol, "EXIT", 0, ts, "Trend_Reversal_Bearish", price, trade_id_to_close=trade.trade_id)

            if state['trend_15m'] != new_trend:
                state['trend_15m'] = new_trend

        elif tf == "5m":
            rsi = data.get('RSI_14')
            if rsi is None: return None

            if state['trend_15m'] == "BULLISH" and rsi < 40:
                state['rsi_setup_5m'] = "OVERSOLD"

            elif state['trend_15m'] == "BEARISH" and rsi > 60:
                state['rsi_setup_5m'] = "OVERBOUGHT"

            else:
                state['rsi_setup_5m'] = "NONE"

        elif tf == "1m":
            ema_10 = data.get('EMA_10')
            if ema_10 is None: return None

            prev_price = state['prev_price_1m']
            state['prev_price_1m'] = price

            if prev_price is None: return None

            trade_quantity = self.config.get('quantity', 1)

            # Entry logic only fires if no trades are open for this symbol.
            if not open_trades and state.get('entry_ready', True):

                # LONG ENTRY
                if (state['trend_15m'] == "BULLISH" and state['rsi_setup_5m'] == "OVERSOLD" and prev_price <= ema_10 and price > ema_10):
                    return self.create_signal(symbol, "BUY", trade_quantity, ts, "RSI_Dip_EMA_Cross_Long", price)

                # SHORT ENTRY
                elif (state['trend_15m'] == "BEARISH" and state['rsi_setup_5m'] == "OVERBOUGHT" and prev_price >= ema_10 and price < ema_10):
                    return self.create_signal(symbol, "SELL", trade_quantity, ts, "RSI_Rally_EMA_Cross_Short", price)

        return None
