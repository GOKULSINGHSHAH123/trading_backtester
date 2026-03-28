"""
Portfolio and Position Management (Optimized Dictionary-Based / High Performance)
"""
import os
import uuid
import json
import glob
import pytz
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Tuple
import polars as pl # type: ignore
from datetime import datetime
from pymongo import MongoClient # type: ignore
from pymongo.database import Database
from pymongo.collection import Collection

from backtester.system.interfaces import Position

ist = pytz.timezone('Asia/Kolkata')

@dataclass
class Portfolio:
    """
    Manages the state of all open and closed trades for all strategies.
    Optimized for O(1) lookups with dictionary-based architecture.
    Includes integrated MTM tracking and processing capabilities.
    """
    # Optimized Structure: Strategy -> Symbol -> Trade ID -> Position (O(1) access)
    open_positions: Dict[str, Dict[str, Dict[str, Position]]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(dict))
    )

    # Flat index for O(1) global lookup: {trade_id: (strategy_id, symbol)}
    active_trade_map: Dict[str, Tuple[str, str]] = field(default_factory=dict)

    # Write buffer for closed trades (flushed periodically to disk)
    closed_trades_list: List[Dict] = field(default_factory=list)

    # File paths (no mtm_log_filename)
    trade_log_filename: str = "trades.csv"
    combined_output_filename: str = "output.json"

    # MTM tracking: {strategy_id: {timestamp: cumulative_pnl}}
    mtm: Dict[str, Dict[Any, float]] = field(
        default_factory=lambda: defaultdict(dict)
    )

    # Realized PnL tracker: {strategy_id: total_realized_pnl}
    realized_pnl_tracker: Dict[str, float] = field(
        default_factory=lambda: defaultdict(float)
    )

    # Cache for closed trades DataFrame
    _cached_df: Optional[pl.DataFrame] = field(init=False, repr=False, default=None)
    _last_count: int = field(init=False, repr=False, default=0)
    
    # Cache for open trades DataFrame
    _cached_open_df: Optional[pl.DataFrame] = field(init=False, repr=False, default=None)
    _is_dirty: bool = field(init=False, repr=False, default=True)

    # MongoDB connection for daily-mode position persistence (populated if STATUS_DB_URI is set)
    _mongo_client: Optional[MongoClient] = field(init=False, repr=False, default=None)
    _db: Optional[Database] = field(init=False, repr=False, default=None)
    _open_positions_collection: Optional[Collection] = field(init=False, repr=False, default=None)


    def __post_init__(self):
        """Initialize clean state, set up MongoDB if available, and remove existing output files."""
        self._cached_df = None
        self._last_count = 0
        self._cached_open_df = None
        self._is_dirty = True
        self._mongo_client = None
        self._db = None
        self._open_positions_collection = None

        # Connect to status DB and load any persisted open positions (daily mode)
        self._initialize_mongodb()
        self._load_open_positions_from_db()

        # Clean up existing output files
        if os.path.exists(self.trade_log_filename):
            os.remove(self.trade_log_filename)

        # Clean up any previous strategy output files
        for f in glob.glob("output_*.json"):
            try:
                os.remove(f)
            except OSError:
                pass

    def _initialize_mongodb(self):
        """Connect to STATUS_DB_URI for position persistence. No-op if URI not set."""
        logger = logging.getLogger(__name__)
        status_uri = os.getenv('STATUS_DB_URI')
        if not status_uri:
            return
        try:
            self._mongo_client = MongoClient(status_uri, serverSelectionTimeoutMS=5000)
            self._db = self._mongo_client["status_manager"]
            self._open_positions_collection = self._db["open_positions"]
            logger.info("Portfolio: MongoDB connected for position persistence")
        except Exception as e:
            logger.warning(f"Portfolio: Could not connect to STATUS_DB_URI: {e}")

    def _load_open_positions_from_db(self):
        """
        Reload persisted open positions from MongoDB into memory.
        Skipped silently when STATUS_DB_URI is not configured.
        """
        logger = logging.getLogger(__name__)
        if self._open_positions_collection is None:
            return
        try:
            docs = list(self._open_positions_collection.find({}))
            if not docs:
                logger.info("Portfolio: No open positions found in DB")
                return
            loaded = 0
            for doc in docs:
                try:
                    strategy_id = doc['strategy_id']
                    symbol      = doc['symbol']
                    trade_id    = doc['trade_id']
                    position = Position(
                        symbol=symbol,
                        trade_id=trade_id,
                        quantity=doc['quantity'],
                        entry_price=doc['entry_price'],
                        entry_timestamp=doc['entry_timestamp'],
                        metadata=doc.get('metadata', {}),
                        running_pnl_points=doc.get('running_pnl_points', 0.0),
                        running_pnl_pct=doc.get('running_pnl_pct', 0.0),
                        last_known_price=doc.get('last_known_price', 0.0),
                        last_update_timestamp=doc.get('last_update_timestamp', doc['entry_timestamp']),
                    )
                    self.open_positions[strategy_id][symbol][trade_id] = position
                    self.active_trade_map[trade_id] = (strategy_id, symbol)
                    loaded += 1
                except KeyError as e:
                    logger.error(f"Portfolio: Skipping invalid position doc, missing field: {e}")
            self._is_dirty = True
            logger.info(f"Portfolio: Restored {loaded} open position(s) from DB")
        except Exception as e:
            logger.error(f"Portfolio: Error loading open positions from DB: {e}")

    def save_all_open_positions_to_db(self):
        """
        Persist ALL current open positions to MongoDB (full replace).
        Called at end of collection when daily_mode is enabled — replaces liquidation.
        No-op when STATUS_DB_URI is not configured.
        """
        logger = logging.getLogger(__name__)
        if self._open_positions_collection is None:
            logger.warning("Portfolio: STATUS_DB_URI not set — cannot save open positions")
            return
        try:
            self._open_positions_collection.delete_many({})
            saved = 0
            for strategy_id, strat_positions in self.open_positions.items():
                for symbol, trades in strat_positions.items():
                    for trade_id, pos in trades.items():
                        self._open_positions_collection.insert_one({
                            'trade_id':             pos.trade_id,
                            'strategy_id':          strategy_id,
                            'symbol':               pos.symbol,
                            'quantity':             pos.quantity,
                            'entry_price':          pos.entry_price,
                            'entry_timestamp':      pos.entry_timestamp,
                            'metadata':             pos.metadata,
                            'running_pnl_points':   pos.running_pnl_points,
                            'running_pnl_pct':      pos.running_pnl_pct,
                            'last_known_price':     pos.last_known_price,
                            'last_update_timestamp': pos.last_update_timestamp,
                        })
                        saved += 1
            if saved:
                logger.info(f"Portfolio: Saved {saved} open position(s) to MongoDB")
            else:
                logger.info("Portfolio: No open positions to save (all closed)")
        except Exception as e:
            logger.error(f"Portfolio: Error saving open positions to MongoDB: {e}")


    @property
    def closed_trades_df(self) -> Optional[pl.DataFrame]:
        """
        Lazy-evaluated DataFrame of closed trades with intelligent caching.
        Regenerated only when new trades are closed.
        """
        current_count = len(self.closed_trades_list)

        # Return cached version if valid
        if self._cached_df is not None and current_count == self._last_count:
            return self._cached_df

        # No closed trades yet
        if current_count == 0:
            return None

        # Regenerate cache
        self._cached_df = pl.DataFrame(self.closed_trades_list)
        self._last_count = current_count
        return self._cached_df


    def get_active_positions(self, strategy_id: str, symbol: Optional[str] = None) -> List[Position]:
        """
        O(1) fetch of live Position objects for strategy and symbol.
        If symbol is None, returns all open positions for the strategy.
        """
        if symbol:
            return list(self.open_positions[strategy_id][symbol].values())
        
        # Aggregate all positions for the strategy
        all_positions = []
        for sym_map in self.open_positions[strategy_id].values():
            all_positions.extend(sym_map.values())
        return all_positions


    def get_active_positions_df(self, strategy_id: str, 
                          symbol: Optional[str] = None) -> Optional[pl.DataFrame]:
        """
        On-the-fly DataFrame view of open trades (fresh running PnL).
        Uses caching to avoid rebuilding DataFrame if no trades have changed.
        Filters by strategy_id and optionally by symbol.
        """
        # Rebuild cache if dirty
        if self._is_dirty:
            all_position_dicts = []
            for s_id, strat_positions in self.open_positions.items():
                for sym_positions in strat_positions.values():
                    for pos in sym_positions.values():
                        # Create a dict representation and inject strategy_id
                        p_data = vars(pos).copy()
                        p_data["strategy_id"] = s_id
                        all_position_dicts.append(p_data)
            
            if all_position_dicts:
                self._cached_open_df = pl.DataFrame(all_position_dicts)
            else:
                self._cached_open_df = None
            
            self._is_dirty = False

        # If cache is still None (no positions), return None
        if self._cached_open_df is None:
            return None

        # Filter the cached DataFrame
        # Note: We filter the *cached* DF. This is fast (Polars).
        filtered = self._cached_open_df.filter(pl.col("strategy_id") == strategy_id)
        
        if symbol:
            filtered = filtered.filter(pl.col("symbol") == symbol)
            
        return filtered if not filtered.is_empty() else None


    def get_closed_positions(self, strategy_id: str, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Returns list of closed trades for a strategy, optionally filtered by symbol.
        Leverages get_closed_positions_df for efficient caching.
        """
        df = self.get_closed_positions_df(strategy_id, symbol)
        if df is None:
            return []
        return df.to_dicts()


    def get_closed_positions_df(self, strategy_id: str, symbol: Optional[str] = None) -> Optional[pl.DataFrame]:
        """
        Returns DataFrame of closed trades for a strategy, optionally filtered by symbol.
        Uses the shared cached_df from closed_trades_df property.
        """
        # Access the master cached dataframe
        df = self.closed_trades_df
        
        if df is None:
            return None

        # Filter by strategy_id
        # Note: 'strategy_id' is added to the log entry in close_position
        filtered = df.filter(pl.col("strategy_id") == strategy_id)

        if symbol:
            # Note: The log entry key is "Symbol" (capitalized) in close_position
            filtered = filtered.filter(pl.col("Symbol") == symbol)

        return filtered if not filtered.is_empty() else None


    def open_position(self, strategy_id: str, symbol: str, quantity: int, 
                   price: float, timestamp: Any, metadata: Dict[str, Any]) -> str:
        """
        Creates new Position and adds to open_positions with O(1) complexity.
        Returns the generated trade_id.
        """
        trade_id = str(uuid.uuid4())
        new_position = Position(
            symbol=symbol,
            trade_id=trade_id,
            quantity=quantity,
            entry_price=price,
            entry_timestamp=timestamp,
            metadata=metadata
        )

        # Store in nested structure
        self.open_positions[strategy_id][symbol][trade_id] = new_position

        # Store in flat index for fast global lookup
        self.active_trade_map[trade_id] = (strategy_id, symbol)

        self._is_dirty = True # Invalidate open trades cache
        return trade_id


    def close_position(self, strategy_id: str, symbol: str, trade_id: str, 
                   exit_price: float, exit_timestamp: Any, 
                   exit_reason: str = "UNKNOWN") -> bool:
        """
        O(1) trade closure: removes from open positions, calculates PnL,
        updates realized PnL tracker, and logs to closed_trades_list.
        """
        logger = logging.getLogger(__name__)

        # Fast lookup using flat index
        if trade_id not in self.active_trade_map:
            return False

        # Verify integrity
        mapped_strat, mapped_symbol = self.active_trade_map[trade_id]
        if mapped_strat != strategy_id or mapped_symbol != symbol:
            logger.error(f"Trade ID integrity error for {trade_id}")
            return False

        # Remove from indices (O(1))
        del self.active_trade_map[trade_id]

        try:
            closed_position = self.open_positions[strategy_id][symbol].pop(trade_id)
        except KeyError:
            logger.error(f"Position missing from storage: {trade_id}")
            return False

        self._is_dirty = True # Invalidate open trades cache

        # Calculate final PnL
        pnl_points = (exit_price - closed_position.entry_price) * closed_position.quantity + 0.0

        # Update realized PnL tracker
        self.realized_pnl_tracker[strategy_id] += pnl_points

        # Create clean log entry
        clean_log = {
            "entry_timestamp": closed_position.entry_timestamp,
            "exit_timestamp": exit_timestamp,
            "Symbol": symbol,
            "EntryPrice": closed_position.entry_price,
            "ExitPrice": exit_price,
            "Quantity": abs(closed_position.quantity),
            "PositionStatus": 1 if closed_position.quantity > 0 else -1,
            "Pnl": pnl_points,
            "strategy_id": strategy_id,
            "exit_reason": exit_reason
        }

        # Append to write buffer
        self.closed_trades_list.append(clean_log)
        return True


    def liquidate_portfolio(self, final_prices: Dict[str, float], 
                                 final_timestamp: Any):
        """Mark-to-market all remaining open positions at collection end."""
        logger = logging.getLogger(__name__)
        logger.info("Closing all open positions at final market prices...")

        # Iterate over snapshot to avoid modification issues
        all_active_trade_ids = list(self.active_trade_map.keys())

        for trade_id in all_active_trade_ids:
            strat_id, symbol = self.active_trade_map[trade_id]
            final_price = final_prices.get(symbol)

            if final_price:
                self.close_position(strat_id, symbol, trade_id, final_price, 
                               final_timestamp, exit_reason="MARK_TO_MARKET")
            else:
                # Fallback: Use last known explicit price to ensure MTM consistency
                try:
                    position = self.open_positions[strat_id][symbol][trade_id]
                    
                    if position.last_known_price > 0:
                        derived_price = position.last_known_price
                        reason = "FORCE_CLOSE_LAST_KNOWN_PRICE"
                    else:
                        # Fallback to entry if never marked (e.g. trade opened on last tick)
                        derived_price = position.entry_price
                        reason = "FORCE_CLOSE_AT_ENTRY"

                    self.close_position(strat_id, symbol, trade_id, derived_price, final_timestamp, exit_reason=reason)
                    logger.warning(f"Force closed trade {trade_id} (Symbol: {symbol}) at fallback price {derived_price:.2f} | {reason}.")
                except KeyError:
                    logger.error(f"Failed to force close trade {trade_id}: Position not found.")


    def mark_to_market(self, strategy_id: str, tick_data: Dict[str, Any], timestamp: Any):
        """
        Efficiently updates running PnL for all open positions of a strategy.
        Used by Fast strategies for bulk accounting updates.
        """
        strat_positions_map = self.open_positions.get(strategy_id)
        if not strat_positions_map:
            return

        for symbol, trades_dict in strat_positions_map.items():
            if not trades_dict:
                continue

            current_price = tick_data.get(symbol)
            if not current_price:
                continue

            # Update all trades for this symbol
            for trade in trades_dict.values():
                pnl_points = (current_price - trade.entry_price) * trade.quantity
                trade.running_pnl_points = pnl_points
                if trade.entry_price != 0:
                    trade.running_pnl_pct = (
                        pnl_points / (trade.entry_price * abs(trade.quantity))
                    ) * 100
                
                # Explicit state tracking for consistency
                trade.last_known_price = current_price
                trade.last_update_timestamp = timestamp

    def store_mtm_snapshot(self, strategy_id: str, tick_data: Dict[str, Any], 
                          timestamp: Any):
        """
        Stores MTM snapshot for the current timestamp.
        Creates equity curve entry: MTM = Realized PnL + Unrealized PnL.
        Optimized to use pre-calculated running_pnl from mark_to_market.
        """
        strat_positions_map = self.open_positions.get(strategy_id)

        # Calculate unrealized PnL from open positions
        unrealized_pnl = 0.0

        if strat_positions_map:
            for trades_dict in strat_positions_map.values():
                for trade in trades_dict.values():
                    unrealized_pnl += getattr(trade, 'running_pnl_points', 0.0)

        # Get realized PnL (all closed trades up to now)
        realized_pnl = self.realized_pnl_tracker.get(strategy_id, 0.0)

        # MTM = Realized + Unrealized (cumulative equity curve)
        cumulative_pnl = realized_pnl + unrealized_pnl

        # Store in MTM dictionary
        self.mtm[strategy_id][timestamp] = cumulative_pnl


    def process_mtm_to_daily(self, strategy_id: str) -> Optional[pl.DataFrame]:
        """
        Processes raw MTM data into daily aggregated format.
        Returns DataFrame with daily MTM calculations.
        """
        if strategy_id not in self.mtm or not self.mtm[strategy_id]:
            return None

        # Convert MTM dict to DataFrame
        df = pl.DataFrame({
            "ti": [int(k) for k in self.mtm[strategy_id].keys()],
            "pnl": list(self.mtm[strategy_id].values())
        })

        # Sort and add datetime (UTC → IST conversion)
        df = (
            df
            .sort("ti")
            .with_columns(
                pl.from_epoch("ti", time_unit="s")
                .dt.replace_time_zone("UTC")
                .dt.convert_time_zone("Asia/Kolkata")
                .alias("dt")
            )
            .with_columns(
                pl.col("dt").dt.truncate("1d").alias("day")
            )
        )

        # Get last PnL of each day (for calculating previous day end)
        daily_end = (
            df
            .group_by("day")
            .agg(pl.col("pnl").last().alias("prev_day_end"))
            .sort("day")
            .with_columns(
                pl.col("prev_day_end").shift(1).fill_null(0).alias("prev_day_end_shifted")
            )
        )

        # Join and calculate intraday MTM
        df = (
            df
            .join(daily_end.select(["day", "prev_day_end_shifted"]), on="day", how="left")
            .with_columns(
                (pl.col("pnl") - pl.col("prev_day_end_shifted")).alias("mtm")
            )
        )

        return df


    def create_mtm_output(self, strategy_id: str) -> List[Dict[str, Any]]:
        """
        Creates MTM output structure compatible with visualization tools.
        """
        df = self.process_mtm_to_daily(strategy_id)
        if df is None:
            return []

        mtm_data = [
            {
                "Unnamed: 0": idx,
                "Date": row["dt"].strftime("%Y-%m-%d %H:%M:%S"),
                "OpenTrades": 0,
                "CapitalInvested": 0,
                "CumulativePnl": round(row["pnl"], 3),
                "mtmPnl": round(row["mtm"], 3),
                "Peak": 0,
                "Drawdown": 0
            }
            for idx, row in enumerate(df.iter_rows(named=True), start=1)
        ]

        return mtm_data


    def create_combined_output(self, strategy_id: str, closed_trades: List[Dict]):
        """
        Creates combined output file with closed trades and MTM data.
        Integrates both data sources into single JSON structure.
        """
        logger = logging.getLogger(__name__)

        if closed_trades is None:
            closed_trades = []

        # Create MTM output
        mtm_data = self.create_mtm_output(strategy_id)

        # Combine into final structure
        final_output = {
            "closedPnl": closed_trades,
            "mtm": mtm_data
        }

        # Save to JSON with strategy-specific filename
        output_filename = f"output_{strategy_id}.json"
        
        try:
            with open(output_filename, 'w') as f:
                json.dump(final_output, f, indent=2)
            logger.info(f"Saved combined output to {output_filename}")
            logger.info(f"- Closed trades: {len(closed_trades)} records")
            logger.info(f"- MTM records: {len(mtm_data)} records")
        except IOError as e:
            logger.error(f"Error saving combined output: {e}")


    def persist_trade_history(self):
        """
        Appends in-memory closed trades to CSV file and clears buffer.
        Optimized for memory efficiency with periodic flushing.
        """
        logger = logging.getLogger(__name__)
        
        # Ensure file exists with headers even if no trades (Artifact requirement)
        if not self.closed_trades_list:
            if not os.path.exists(self.trade_log_filename):
                try:
                    with open(self.trade_log_filename, 'w', encoding='utf-8') as f:
                        f.write("Key,ExitTime,Symbol,EntryPrice,ExitPrice,Quantity,PositionStatus,Pnl,ExitType\n")
                    logger.info(f"Created empty trade log at {self.trade_log_filename}")
                except IOError as e:
                    logger.error(f"Error creating empty trade log: {e}")
            return

        # Create DataFrame from buffer
        df_raw = pl.DataFrame(self.closed_trades_list)

        # Transform raw columns to legacy CSV format
        df_to_flush = df_raw.with_columns([
            pl.col("entry_timestamp").map_elements(
                lambda ts: datetime.fromtimestamp(ts, ist).strftime('%Y-%m-%d %H:%M:%S'),
                return_dtype=pl.String
            ).alias("Key"),

            pl.col("exit_timestamp").map_elements(
                lambda ts: datetime.fromtimestamp(ts, ist).strftime('%Y-%m-%d %H:%M:%S'),
                return_dtype=pl.String
            ).alias("ExitTime"),

            pl.format("{}: {}", pl.col("strategy_id"), pl.col("exit_reason")).alias("ExitType")
        ]).select([
            "Key", "ExitTime", "Symbol", "EntryPrice", "ExitPrice",
            "Quantity", "PositionStatus", "Pnl", "ExitType"
        ])

        try:
            # Check file size to determine if header is needed
            file_exists = os.path.exists(self.trade_log_filename)
            file_size = os.path.getsize(self.trade_log_filename) if file_exists else 0
            is_empty = file_size == 0

            with open(self.trade_log_filename, 'a', encoding='utf-8') as f:
                df_to_flush.write_csv(f, include_header=is_empty)
        except IOError as e:
            logger.error(f"Error flushing trades to disk: {e}")
            return

        # Clear buffer and invalidate cache
        self.closed_trades_list.clear()
        self._cached_df = None
        logger.info(f"Flushed {len(df_to_flush)} closed trades to {self.trade_log_filename}")


    def summarize_results(self):
        """
        Reads complete trade log, calculates performance metrics,
        and prints comprehensive summary to console.
        Optimized to read trades.csv only once.
        """
        logger = logging.getLogger(__name__)

        # Ensure trades are flushed to disk first
        if self.closed_trades_list:
            self.persist_trade_history()

        # Check if trades exist
        if not os.path.exists(self.trade_log_filename) or \
           os.path.getsize(self.trade_log_filename) == 0:
            logger.info("\nNo trades were logged during simulation.")
            # Even with no trades, we might want to dump MTM (equity curve)
            for strategy_id in self.mtm.keys():
                self.create_combined_output(strategy_id, [])
            return

        # Read and process final CSV ONCE
        try:
            df = pl.read_csv(self.trade_log_filename)
            
            # Sort chronologically by Key (Entry Time) & round off Pnl to 3 decimals
            df = df.sort("Key")
            df = df.with_columns((pl.col("Pnl").round(3) + 0.0).alias("Pnl"))
            
            # Overwrite the file with sorted data
            df.write_csv(self.trade_log_filename)
            # Process for summary (extract strategy_id)
            df = df.with_columns(
                pl.col("ExitType").str.split_exact(by=": ", n=1)
                .struct.rename_fields(["strategy_id", "reason"])
            ).unnest("ExitType")
            
            # Create combined output for each strategy
            for strategy_id in self.mtm.keys():
                # Filter trades for this specific strategy
                strat_trades_df = df.filter(pl.col("strategy_id") == strategy_id)
                # Convert to dicts for JSON output
                closed_trades = strat_trades_df.drop("strategy_id").to_dicts()
                self.create_combined_output(strategy_id, closed_trades)

        except Exception as e:
            logger.error(f"Error reading final trade log: {e}")
            return

        # Per-strategy breakdown
        logger.info("\n" + "=" * 60)
        logger.info("PER-STRATEGY PERFORMANCE BREAKDOWN")
        logger.info("=" * 60)

        for strategy_id, strategy_df in df.group_by("strategy_id"):
            total_trades = len(strategy_df)
            winning_trades = strategy_df.filter(pl.col("Pnl") > 0)
            win_rate = (len(winning_trades) / total_trades * 100) if total_trades > 0 else 0
            total_pnl = strategy_df["Pnl"].sum()

            logger.info(f"\n[#] Strategy: {strategy_id}")
            logger.info(f"   Total Trades:         {total_trades}")
            logger.info(f"   Win Rate:             {win_rate:.2f}%")
            logger.info(f"   Total Net PnL (Points): {total_pnl:.2f}")

        # Portfolio summary
        total_trades = len(df)
        winning_trades = df.filter(pl.col("Pnl") > 0)
        num_winning = len(winning_trades)
        num_losing = total_trades - num_winning
        win_rate = (num_winning / total_trades * 100) if total_trades > 0 else 0
        total_pnl = df["Pnl"].sum()

        logger.info("\n" + "=" * 60)
        logger.info("FINAL PORTFOLIO PERFORMANCE SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Total Trades:                   {total_trades}")
        logger.info(f"Winning Trades:                 {num_winning}")
        logger.info(f"Losing Trades:                  {num_losing}")
        logger.info(f"Win Rate:                       {win_rate:.2f}%")
        logger.info(f"Total Net PnL (Points):         {total_pnl:.2f}")
        logger.info("=" * 60)

        logger.info(f"\nSaved {len(df)} trades to {self.trade_log_filename}")
        logger.info("Final Trade Log:\n" + str(df.head()))


    def get_daily_summary(self, strategy_id: str) -> Optional[pl.DataFrame]:
        """
        Returns daily aggregated summary for a strategy.
        Useful for performance analysis and visualization.
        """
        df = self.process_mtm_to_daily(strategy_id)
        if df is None:
            return None

        # Aggregate to daily level
        daily_summary = (
            df
            .group_by("day")
            .agg([
                pl.col("pnl").last().alias("cumulative_pnl"),
                pl.col("ti").count().alias("count")
            ])
            .sort("day")
            .with_columns(
                (pl.col("cumulative_pnl") - pl.col("cumulative_pnl").shift(1).fill_null(0))
                .alias("daily_mtm")
            )
        )

        return daily_summary
