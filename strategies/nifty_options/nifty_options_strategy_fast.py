"""
High-Performance Nifty Options Strategy (Optimized with Nested Dict Structure)
==============================================================================
Cross-Asset execution with clean architecture and nested dict portfolio access.
FIXED: Expiry day exit now checks individual position expiry dates.
"""


from typing import List, Optional, Tuple
from datetime import datetime, timezone, timedelta
import polars as pl # type: ignore

from backtester.system.strategy import BaseStrategyFast, Signal
from backtester.system.interfaces import IPortfolio



class NiftyOptionsStrategyFast(BaseStrategyFast):
    def __init__(self, strategy_id: str, config: dict):
        super().__init__(strategy_id, config)

        # Strategy Parameters
        self.quantity = self.config.get('quantity', 75)
        self.stop_loss_pct = self.config.get('stop_loss_pct', 0.3)
        self.max_pe_count = self.config.get('max_pe_count', 3)
        self.max_ce_count = self.config.get('max_ce_count', 3)

        # Internal state
        self.last_processed_ts = -1



    @staticmethod
    def get_data_transform(df: pl.DataFrame, timeframe: str) -> pl.DataFrame:
        """
        Pre-calculates RSI and Crossover signals on NIFTY 50 data.
        """
        df = df.with_columns(
            pl.from_epoch(pl.col("ti"), time_unit="s")
            .dt.strftime("%d%b%y")
            .str.to_uppercase()
            .alias("ti_str")
        )


        if timeframe != '5m':
            return df


        # RSI 14 Calculation
        delta = df['c'].diff()
        up = delta.clip(lower_bound=0)
        down = -1 * delta.clip(upper_bound=0)

        ema_up = up.ewm_mean(com=13, adjust=False, min_periods=14)
        ema_down = down.ewm_mean(com=13, adjust=False, min_periods=14)

        rs = ema_up / ema_down
        rsi = 100 - (100 / (1 + rs))

        # Crossover Signals
        prev_rsi = rsi.shift(1)

        rsi_signal = (pl.when((prev_rsi >= 60) & (rsi < 60)).then(-1)
                      .when((prev_rsi <= 40) & (rsi > 40)).then(1)
                      .otherwise(0))


        return df.with_columns([
            rsi.alias("RSI_14"),
            rsi_signal.alias("RSI_Crossover"),
            pl.min_horizontal([pl.col("l"), pl.col("l").shift(1)]).alias("min_low"),
            pl.max_horizontal([pl.col("h"), pl.col("h").shift(1)]).alias("max_high")
        ])



    def on_packet(self, packet: dict, execution_data: dict) -> List[Signal]:
        """
        Main strategy logic - handles PE/CE specific logic with clean DataFrame access.
        """
        signals = []
        timestamp = packet['timestamp']

        # Get 5m data specifically for RSI/Entry logic
        tf_5m = packet.get('5m', {})
        tf_5m_data = tf_5m.get('current_data', {})

        # Timestamp Deduplication
        is_new_timestamp = (timestamp != self.last_processed_ts)

        # Get all open positions as list of dicts (flat structure)
        open_positions_df = self.get_positions_df()
        all_open_positions = open_positions_df.to_dicts() if open_positions_df is not None else []

        # Fetch current execution prices once
        current_execution_prices = execution_data

        # --- 1. Global Risk Management ---
        # Run risk management once per timestamp using any available spot data for underlying reference
        if is_new_timestamp:
            self.last_processed_ts = timestamp

            # Use '5m' NIFTY 50 spot data if available, else look for it in other timeframes
            ref_spot_data = tf_5m_data.get('NIFTY 50')
            if not ref_spot_data:
                for tf in packet:
                    if tf == 'timestamp': continue
                    ref_spot_data = packet[tf].get('current_data', {}).get('NIFTY 50')
                    if ref_spot_data: break

            if all_open_positions:
                signals.extend(
                    self._check_risk_management(
                        all_open_positions,
                        current_execution_prices,
                        ref_spot_data,
                        timestamp
                    )
                )

        # --- 2. Entry Logic (Only if 5m data is present) ---
        if not tf_5m_data:
            return signals

        for _ in tf_5m_data.keys():
            symbol_mapping = {
                "NIFTY BANK" : "BANKNIFTY",
                "NIFTY 50" : "NIFTY",
                "SENSEX" : "SENSEX"
            }

            symbol = symbol_mapping.get(_)
            if symbol is None: continue # Skip if it's not a mapped index

            spot_data = tf_5m_data.get(_)

            rsi_signal = spot_data.get('RSI_Crossover', 0)
            if rsi_signal == 0:
                continue

            # Count PE/CE positions using flat list
            pe_count, ce_count = self._count_option_positions(all_open_positions)

            if rsi_signal != 0:
                self.log(f"RSI Signal detected: {rsi_signal} for {symbol}. Current Counts - PE: {pe_count}, CE: {ce_count}", level="DEBUG")

            # Bearish Signal (Buy PE)
            if rsi_signal == -1 and pe_count < self.max_pe_count:
                sig = self._create_option_signal(symbol=symbol, spot_data=spot_data, execution_data=execution_data, option_type="PE", ts=timestamp)
                if sig:
                    signals.append(sig)

            # Bullish Signal (Sell CE)
            elif rsi_signal == 1 and ce_count < self.max_ce_count:
                sig = self._create_option_signal(symbol=symbol, spot_data=spot_data, execution_data=execution_data, option_type="CE", ts=timestamp)
                if sig:
                    signals.append(sig)

        return signals


    def _count_option_positions(self, open_positions: List[dict]) -> Tuple[int, int]:
        """
        Counts PE and CE positions from flat list of position dicts.
        """
        pe_count = 0
        ce_count = 0

        for trade in open_positions:
            # Metadata is nested in the 'metadata' field
            metadata = trade.get('metadata', {})
            option_type = metadata.get('type', '')

            if option_type == 'PE':
                pe_count += 1
            elif option_type == 'CE':
                ce_count += 1

        return pe_count, ce_count


    def _check_risk_management(
        self,
        open_positions: List[dict],
        current_prices: dict,
        nifty_spot_data: Optional[dict],
        timestamp: int
    ) -> List[Signal]:
        """
        Checks risk management rules for all open positions using flat list.
        """
        signals = []

        # Extract NIFTY spot price
        nifty_spot_price = 0.0
        if nifty_spot_data:
            nifty_spot_price = nifty_spot_data.get('c', 0.0)
        else:
            # Fallback (though current_prices might not have index data depending on config)
            nifty_exec = current_prices.get('NIFTY 50')
            if nifty_exec:
                nifty_spot_price = nifty_exec.get('c', 0.0)

        # Get current date string for expiry comparison
        current_date_str = ''
        if nifty_spot_data:
            current_date_str = nifty_spot_data.get('ti_str', '')

        # Check if it's 3:20 PM IST (expiry exit time)
        ist = timezone(timedelta(hours=5, minutes=30))
        dt = datetime.fromtimestamp(timestamp, tz=ist)
        is_exit_time = (dt.hour == 15 and dt.minute == 20)  # 3:20 PM

        # Iterate through all open positions
        for trade in open_positions:
            opt_symbol = trade['symbol']

            # Get current option price (O(1) lookup)
            opt_price_data = current_prices.get(opt_symbol)
            if not opt_price_data or 'c' not in opt_price_data:
                continue

            current_opt_price = opt_price_data['c']
            entry_price = trade['entry_price']
            quantity = trade['quantity']
            trade_id = trade['trade_id']
            metadata = trade.get('metadata', {})

            # A. Expiry Day Exit at 3:20 PM
            if is_exit_time and current_date_str:
                position_expiry = metadata.get('expiry', '')

                # Only exit if the position's expiry matches today's date
                if position_expiry == current_date_str:
                    self.log(f"EXPIRY EXIT triggered for {opt_symbol} (Expiry: {position_expiry}) at {current_opt_price}")
                    signals.append(self.create_signal(
                        opt_symbol, "EXIT", 0, timestamp, "EXPIRY_EXIT", current_opt_price,
                        trade_id_to_close=trade_id
                    ))
                    continue

            # B. Percentage Stop Loss
            trade_action = "BUY" if quantity > 0 else "SELL"
            is_sl_hit = False
            sl_price = 0.0

            if trade_action == "BUY":
                sl_price = entry_price * (1 - self.stop_loss_pct)
                if current_opt_price <= sl_price:
                    is_sl_hit = True
            else:
                sl_price = entry_price * (1 + self.stop_loss_pct)
                if current_opt_price >= sl_price:
                    is_sl_hit = True

            if is_sl_hit:
                self.log(f"STOP LOSS hit for {opt_symbol} ({trade_action}). Entry: {entry_price}, Current: {current_opt_price}, SL: {sl_price:.2f}", level="WARNING")
                signals.append(self.create_signal(
                    opt_symbol, "EXIT", 0, timestamp, "STOP_LOSS_PCT", current_opt_price,
                    trade_id_to_close=trade_id
                ))
                continue

            # C. Candle/Spot Based Stop Loss (PE/CE specific logic)
            if nifty_spot_price > 0:
                candle_stop = metadata.get('candle_stop_loss')
                option_type = metadata.get('type')

                if candle_stop and candle_stop > 0:
                    if option_type == 'PE' and nifty_spot_price < candle_stop:
                        self.log(f"CANDLE SL hit for {opt_symbol} (PE). Spot: {nifty_spot_price}, Trigger: {candle_stop}", level="WARNING")
                        signals.append(self.create_signal(
                            opt_symbol, "EXIT", 0, timestamp, "CANDLE_SL_SPOT", current_opt_price,
                            trade_id_to_close=trade_id
                        ))
                    elif option_type == 'CE' and nifty_spot_price > candle_stop:
                        self.log(f"CANDLE SL hit for {opt_symbol} (CE). Spot: {nifty_spot_price}, Trigger: {candle_stop}", level="WARNING")
                        signals.append(self.create_signal(
                            opt_symbol, "EXIT", 0, timestamp, "CANDLE_SL_SPOT", current_opt_price,
                            trade_id_to_close=trade_id
                        ))

        return signals



    def _create_option_signal(
        self,
        symbol:str,
        spot_data: dict,
        execution_data: dict,
        option_type: str,
        ts: int
    ) -> Optional[Signal]:
        """
        Creates an option entry signal with metadata.
        """
        spot_price = spot_data.get('c')
        if spot_price is None:
            return None

        # 1. Select Strike
        strike_price = int(round(spot_price / 50) * 50)

        # 2. Determine Expiry
        current_date_str = spot_data.get('ti_str', '')

        # If current date is expiry day, use next expiry
        if current_date_str == spot_data.get('CurrentExpiry') :
            expiry = spot_data.get('NextExpiry')
        else:
            expiry = spot_data.get('CurrentExpiry')

        # 3. Construct Symbol
        strike_symbol = f"{symbol}{expiry}{strike_price}{option_type}"

        # 4. Check Data Availability
        # execution_data is now a pre-resolved dict for the current timestamp: { symbol: {c: ...} }
        opt_data = execution_data.get(strike_symbol)
        if not opt_data or 'c' not in opt_data:
            self.log(f"Data missing for selected option: {strike_symbol} at {ts}", level="WARNING")
            return None

        option_price = opt_data['c']

        # 5. Action (PE=Buy, CE=Sell)
        action = "BUY" if option_type == "PE" else "SELL"

        # 6. Metadata stored in trade.metadata dict
        candle_stop = spot_data.get('min_low', 0.0) if option_type == 'PE' else spot_data.get('max_high', 0.0)

        self.log(f"Generating {action} signal for {strike_symbol} at {option_price}. Spot: {spot_price}, Strike: {strike_price}, Expiry: {expiry}")

        # FIX: Store expiry date in metadata for proper exit logic
        return self.create_signal(
            symbol=strike_symbol,
            action=action,
            quantity=self.quantity,
            timestamp=ts,
            reason="RSI_CROSS_ENTRY",
            price=option_price,
            custom_metadata={
                "type": option_type,
                "expiry": expiry,  # ← CRITICAL: Store expiry for risk management
                "candle_stop_loss": candle_stop,
                "strike": float(strike_price),
                "underlying_entry": spot_price
            }
        )
