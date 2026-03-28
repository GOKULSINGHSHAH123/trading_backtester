"""
Pair Trading Strategy (Fast Mode - Institutional Version)
=========================================================
Implements a mean-reversion logic on the ratio of two assets.

Architecture Note:
- `get_global_transform`: Calculates the Ratio and EMA Crossover using full
  vectorized operations on the aggregated dataset. This creates a synthetic
  'ratio_crossover' indicator attached to the Primary symbol.
- `on_packet`: Simply executes trades based on the pre-computed signal.
"""

from typing import List, Optional, Dict, Tuple
from collections import defaultdict
import polars as pl # type: ignore

from backtester.system.strategy import BaseStrategyFast, Signal
from backtester.system.interfaces import IPortfolio

class PairTraderFast(BaseStrategyFast):
    # Defining pairs at class level for access in static context
    PAIRS = [
        ("ACC", "AMBUJACEM"),
        ("ADANIENT", "ADANIPORTS"),
        ("AARTIIND", "ATUL"),
        ("ALKEM", "AJANTPHARM")
    ]

    def __init__(self, strategy_id: str, config: dict):
        super().__init__(strategy_id, config)
        self.quantity = self.config.get("quantity", 100)
        self.state = defaultdict(lambda: {"prev_cross": 0})

    @staticmethod
    def get_global_transform(data_map: Dict[str, Dict[str, pl.DataFrame]]) -> Dict[str, Dict[str, pl.DataFrame]]:
        """
        Global Batch Transform.
        Receives the full dictionary of all loaded instruments for the current batch.
        Calculates Ratios and EMAs vectorially.
        """
        print(f"   [PairTrader] Executing Global Transform on {len(data_map)} instruments...")

        for p1, p2 in PairTraderFast.PAIRS:
            # We operate on the '1m' timeframe primarily
            timeframe = "1m"

            # Check if both legs are present in this batch
            if p1 not in data_map or p2 not in data_map:
                continue

            df1 = data_map[p1].get(timeframe)
            df2 = data_map[p2].get(timeframe)

            if df1 is None or df2 is None or df1.is_empty() or df2.is_empty():
                continue

            # Naming conventions as per instructions
            ratio_col = f"ratio_{p1}{p2}"
            ema10_col = f"ema10_{p1}{p2}"
            ema30_col = f"ema30_{p1}{p2}"
            cross_col = f"crossover_{p1}{p2}"

            # Join on timestamp
            combined = df1.join(df2.select(["ti", "c"]), on="ti", suffix="_other")

            # Vectorized Ratio Calculation: (P1 / P2) * 1000
            combined = combined.with_columns(
                (pl.col("c") / pl.col("c_other") * 1000).alias(ratio_col)
            )

            # Vectorized EMA Calculation
            alpha_10 = 2 / (10 + 1)
            alpha_30 = 2 / (30 + 1)

            combined = combined.with_columns([
                pl.col(ratio_col).ewm_mean(alpha=alpha_10, adjust=False).alias(ema10_col),
                pl.col(ratio_col).ewm_mean(alpha=alpha_30, adjust=False).alias(ema30_col)
            ])

            # Vectorized Signal Generation
            combined = combined.with_columns(
                pl.when(pl.col(ema10_col) > pl.col(ema30_col)).then(1)
                .when(pl.col(ema10_col) < pl.col(ema30_col)).then(-1)
                .otherwise(0)
                .alias(cross_col)
            )

            # Update the Primary Symbol's DataFrame in the map
            # We keep all original columns plus our new indicators
            data_map[p1][timeframe] = combined.drop(["c_other"])
        return data_map

    def on_packet(self, packet: dict, execution_data: dict) -> List[Signal]:
        signals = []
        timestamp = packet['timestamp']

        # This strategy operates primarily on the '1m' timeframe
        tf_1m = packet.get('1m', {})
        current_data = tf_1m.get('current_data', {})

        if not current_data:
            return signals

        # Use clean DF access helper
        # Convert to list of dicts for easier iteration
        open_positions_df = self.get_positions_df()
        all_open_positions_list = open_positions_df.to_dicts() if open_positions_df is not None else []

        # Convert list to dict for faster lookup by symbol
        # Structure: {symbol: [trade1, trade2, ...]}
        all_open_positions = defaultdict(list)
        for trade in all_open_positions_list:
            all_open_positions[trade['symbol']].append(trade)

        for p1, p2 in self.PAIRS:
            # We check the Primary symbol for the signal
            if p1 not in current_data or p2 not in current_data:
                continue

            data_p1 = current_data[p1]

            # Extract pre-computed signal using dynamic column name
            cross_col = f"crossover_{p1}{p2}"
            current_cross = data_p1.get(cross_col, 0)
            state = self.state[p1]

            if current_cross != 0 and current_cross != state["prev_cross"]:

                # 1. Close Existing Positions for this pair
                self._close_pair_positions(p1, p2, all_open_positions, signals, timestamp, current_data)

                # 2. Enter New Positions
                price_p1 = data_p1['c']
                price_p2 = current_data[p2]['c']

                if current_cross == 1:
                    # Bullish: Buy Leg 1, Sell Leg 2
                    signals.append(self.create_signal(p1, "BUY", self.quantity, timestamp, "PAIR_CROSS_BULL", price_p1))
                    signals.append(self.create_signal(p2, "SELL", self.quantity, timestamp, "PAIR_CROSS_BULL", price_p2))

                elif current_cross == -1:
                    # Bearish: Sell Leg 1, Buy Leg 2
                    signals.append(self.create_signal(p1, "SELL", self.quantity, timestamp, "PAIR_CROSS_BEAR", price_p1))
                    signals.append(self.create_signal(p2, "BUY", self.quantity, timestamp, "PAIR_CROSS_BEAR", price_p2))

                state["prev_cross"] = current_cross

        return signals

    def _close_pair_positions(self, sym1: str, sym2: str, all_positions: dict, signals: list, ts: int, current_data: dict):
        """Helper to generate EXIT signals for any open trades in the pair."""
        for sym in [sym1, sym2]:
            if sym in all_positions:
                price = current_data[sym]['c']
                for trade in all_positions[sym]:
                    signals.append(self.create_signal(
                        sym, "EXIT", 0, ts, "PAIR_REVERSAL", price, trade_id_to_close=trade['trade_id'] # type: ignore
                    ))
