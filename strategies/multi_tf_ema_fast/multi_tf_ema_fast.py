"""
Multi-Timeframe EMA Strategy (Fast Mode)
========================================
Calculates EMA 100 on 15m, 30m, 1h, and 2h candles.
Buys when Close > EMA.
Sells when Close < EMA.
Operates independently on each timeframe.
"""

from typing import List
import polars as pl # type: ignore
from backtester.system.strategy import BaseStrategyFast, Signal
from backtester.system.interfaces import IPortfolio

class MultiTimeframeEmaStrategy(BaseStrategyFast):
    def __init__(self, strategy_id: str, config: dict):
        super().__init__(strategy_id, config)
        self.quantity = self.config.get("parameters", {}).get("quantity", 10)

    @staticmethod
    def get_data_transform(df: pl.DataFrame, timeframe: str) -> pl.DataFrame:
        """
        Calculates EMA 100 for the specific timeframe.
        Example: Creates 'ema_100_15m' for the 15m dataframe.
        """
        ema_col = f"ema_100_{timeframe}"
        return df.with_columns(
            pl.col("c").ewm_mean(span=100, adjust=False).alias(ema_col)
        )

    def _handle_sl_tp(self, symbol: str, tf: str, data: dict, current_tf_position, signals: list, timestamp: int):
        """
        Calculates SL/TP based on entry price and current packet price.
        """
        params = self.config.get("parameters", {})
        sl_pct = params.get("stop_loss", 0.0)
        tp_pct = params.get("take_profit", 0.0)

        if not current_tf_position or (sl_pct <= 0 and tp_pct <= 0):
            return

        entry_price = current_tf_position.entry_price
        current_price = data['c']
        qty = current_tf_position.quantity

        # Calculate PnL percentage
        if qty > 0:
            pnl_pct = (current_price - entry_price) / entry_price
        else:
            pnl_pct = (entry_price - current_price) / entry_price

        # Check SL
        if sl_pct > 0 and pnl_pct <= -sl_pct:
            signals.append(self.create_signal(
                symbol, "EXIT", 0, timestamp, f"STOP_LOSS_{tf}", current_price, # type: ignore
                trade_id_to_close=current_tf_position.trade_id
            ))
            self.log(f"SL triggered for {symbol} on {tf}. PnL: {pnl_pct:.2%}", symbol=symbol)

        # Check TP
        elif tp_pct > 0 and pnl_pct >= tp_pct:
            signals.append(self.create_signal(
                symbol, "EXIT", 0, timestamp, f"TAKE_PROFIT_{tf}", current_price, # type: ignore
                trade_id_to_close=current_tf_position.trade_id
            ))
            self.log(f"TP triggered for {symbol} on {tf}. PnL: {pnl_pct:.2%}", symbol=symbol)

    def on_packet(self, packet: dict, execution_data: dict) -> List[Signal]:
        signals = []
        timestamp = packet['timestamp']

        # Get all trades for this strategy as DataFrame once per packet
        all_open_trades_df = self.get_positions_df()

        # Iterate over all timeframes present in the nested packet
        for tf, tf_content in packet.items():
            if tf == 'timestamp':
                continue

            current_data = tf_content.get('current_data', {})
            # Explicitly define the indicator column we are looking for in this timeframe
            ema_col = f"ema_100_{tf}"

            for symbol, data in current_data.items():
                # Safety check if EMA was calculated for this specific timeframe
                if ema_col not in data:
                    continue

                close = data['c']
                ema = data[ema_col]

                # Check for existing position for this SPECIFIC timeframe
                current_tf_position = None
                if all_open_trades_df is not None:
                    # Efficient approach: Filter by symbol first
                    symbol_trades = all_open_trades_df.filter(pl.col("symbol") == symbol)

                    for trade in symbol_trades.iter_rows(named=True):
                        meta = trade.get('metadata', {})
                        if meta and meta.get("timeframe") == tf:
                            current_tf_position = trade
                            break

                # BULLISH Condition (Close > EMA)
                if close > ema:
                    if current_tf_position:
                        # Access via dictionary key (DataFrame row)
                        if current_tf_position['quantity'] < 0:
                            # We are Short -> Reverse to Long
                            # 1. Close Short
                            signals.append(self.create_signal(
                                symbol, "EXIT", 0, timestamp,
                                f"REVERSAL_TO_LONG_{ema_col}", close,
                                trade_id_to_close=current_tf_position['trade_id']
                            ))
                            # 2. Open Long
                            signals.append(self.create_signal(
                                symbol, "BUY", self.quantity, timestamp,
                                f"CROSS_ABOVE_{ema_col}", close,
                                custom_metadata={"timeframe": tf, "indicator": ema_col}
                            ))
                            self.log(f"Reversing Short to Long for {symbol} ({tf}). Close: {close}, EMA: {ema}", symbol=symbol)
                    else:
                        # We are Flat -> Open Long
                        signals.append(self.create_signal(
                            symbol, "BUY", self.quantity, timestamp,
                            f"CROSS_ABOVE_{ema_col}", close,
                            custom_metadata={"timeframe": tf, "indicator": ema_col}
                        ))
                        self.log(f"Opening Long for {symbol} ({tf}). Close: {close}, EMA: {ema}", symbol=symbol)

                # BEARISH Condition (Close < EMA)
                elif close < ema:
                    if current_tf_position:
                        if current_tf_position['quantity'] > 0:
                            # We are Long -> Reverse to Short
                            # 1. Close Long
                            signals.append(self.create_signal(
                                symbol, "EXIT", 0, timestamp,
                                f"REVERSAL_TO_SHORT_{ema_col}", close,
                                trade_id_to_close=current_tf_position['trade_id']
                            ))
                            # 2. Open Short
                            signals.append(self.create_signal(
                                symbol, "SELL", self.quantity, timestamp,
                                f"CROSS_BELOW_{ema_col}", close,
                                custom_metadata={"timeframe": tf, "indicator": ema_col}
                            ))
                            self.log(f"Reversing Long to Short for {symbol} ({tf}). Close: {close}, EMA: {ema}", symbol=symbol)
                    else:
                        # We are Flat -> Open Short
                        signals.append(self.create_signal(
                            symbol, "SELL", self.quantity, timestamp,
                            f"CROSS_BELOW_{ema_col}", close,
                            custom_metadata={"timeframe": tf, "indicator": ema_col}
                        ))
                        self.log(f"Opening Short for {symbol} ({tf}). Close: {close}, EMA: {ema}", symbol=symbol)

        return signals
