"""
Base Strategy class (v6)
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any, List, Union, Callable
from collections import defaultdict
import logging
import os
import polars as pl # type: ignore
from pymongo import MongoClient # type: ignore

from backtester.system.interfaces import Position, IPortfolio

@dataclass
class Signal:
    strategy_id: str
    symbol: str
    action: str  # BUY, SELL, EXIT
    timestamp: datetime
    quantity: int
    metadata: Dict[str, Any]
    trade_id_to_close: Optional[str] = None


class BaseStrategy(ABC):
    def __init__(self, strategy_id: str, config: dict):
        self.strategy_id = strategy_id
        self.config = config
        self.state = defaultdict(dict)
        self.logger_factory: Optional[Callable[[str], logging.Logger]] = None

    def set_logger_factory(self, factory: Callable[[str], logging.Logger]):
        """Injects the logger factory for dynamic logging (strategy or symbol scoped)."""
        self.logger_factory = factory

    def log(self, message: str, symbol: Optional[str] = None, level: str = "INFO"):
        """
        Logs a message to the strategy-specific or symbol-specific log file.
        If symbol is provided, writes only to {symbol}_{strategy_id}.log.
        If symbol is None, writes to {strategy_id}.log.
        """
        if self.logger_factory:
            # Determine logger name based on whether symbol is provided
            # Symbol-scoped logs now include strategy_id for better isolation
            logger_name = f"{symbol}_{self.strategy_id}" if symbol else self.strategy_id
            logger = self.logger_factory(logger_name)
            
            # Prepend strategy ID for clarity
            formatted_message = f"[{self.strategy_id}] {message}"
            
            if level.upper() == "ERROR":
                logger.error(formatted_message)
            elif level.upper() == "WARNING":
                logger.warning(formatted_message)
            elif level.upper() == "DEBUG":
                logger.debug(formatted_message)
            else:
                logger.info(formatted_message)

    def set_portfolio(self, portfolio: IPortfolio):
        """Injects the portfolio instance for internal helper access."""
        self.portfolio = portfolio

    def get_positions(self, symbol: Optional[str] = None) -> List[Position]:
        """
        Get active positions for this strategy.
        If symbol is provided, returns positions for that symbol.
        If symbol is None, returns all active positions for the strategy.
        """
        if not hasattr(self, 'portfolio'):
             raise RuntimeError("Portfolio not injected into strategy.")
        return self.portfolio.get_active_positions(self.strategy_id, symbol)

    def get_positions_df(self, symbol: Optional[str] = None) -> Optional[pl.DataFrame]:
        """Get DataFrame view of open positions."""
        if not hasattr(self, 'portfolio'):
             raise RuntimeError("Portfolio not injected into strategy.")
        return self.portfolio.get_active_positions_df(self.strategy_id, symbol)

    def get_closed_positions(self, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get closed positions for this strategy.
        If symbol is provided, returns closed positions for that symbol.
        """
        if not hasattr(self, 'portfolio'):
             raise RuntimeError("Portfolio not injected into strategy.")
        return self.portfolio.get_closed_positions(self.strategy_id, symbol)

    def get_closed_positions_df(self, symbol: Optional[str] = None) -> Optional[pl.DataFrame]:
        """
        Get DataFrame view of closed positions for this strategy.
        """
        if not hasattr(self, 'portfolio'):
             raise RuntimeError("Portfolio not injected into strategy.")
        return self.portfolio.get_closed_positions_df(self.strategy_id, symbol)

    @staticmethod
    def get_data_transform(df: pl.DataFrame, timeframe: str) -> pl.DataFrame:
        """
        Pure function to transform raw OHLCV data.
        Must be static to ensure multiprocessing compatibility.
        Strategies should override this to add indicators.
        """
        return df

    @staticmethod
    def get_global_transform(data_map: Dict[str, Dict[str, pl.DataFrame]]) -> Dict[str, Dict[str, pl.DataFrame]]:
        """
        Optional global hook.
        Receives the full dictionary of loaded DataFrames for the current batch.
        Format: { "SYMBOL": { "1m": df, "5m": df } }
        Returns: Modified dictionary (can include new synthetic symbols).
        
        Use this for multi-asset calculations (spreads, correlations) 
        that require visibility of the entire batch.
        """
        return data_map

    def create_signal(
        self,
        symbol: str,
        action: str,
        quantity: int,
        timestamp: datetime,
        reason: str,
        price: float,
        custom_metadata: Optional[Dict[str, Any]] = None,
        trade_id_to_close: Optional[str] = None
    ) -> Signal:
        """Factory method to create a new signal."""
        final_metadata = {"reason": reason, "trigger_price": price}
        if custom_metadata:
            final_metadata.update(custom_metadata)

        return Signal(
            strategy_id=self.strategy_id,
            symbol=symbol,
            action=action,
            timestamp=timestamp,
            quantity=quantity,
            metadata=final_metadata,
            trade_id_to_close=trade_id_to_close,
        )

    @abstractmethod
    def on_trigger(self, data: dict, execution_data: dict) -> Optional[Union[Signal, List[Signal]]]:
        """
        Processes a single data point for a specific symbol.
        
        Args:
            data (dict): The candle/tick data for the symbol.
            execution_data (dict): The pre-resolved execution map for the current tick.
                                 Format: { symbol: {c: price, ...} }
        """
        pass

class BaseStrategyFast(BaseStrategy):
    """
    Base class for High-Performance strategies that process the entire
    data packet at once, bypassing framework-level flattening and iteration.

    When STATUS_DB_URI is set in the environment, this class automatically:
    - Loads strategy state (instance attributes) from MongoDB on startup
    - Saves strategy state back to MongoDB at end of each collection (daily mode)

    Child classes should override _setup_state_defaults() to declare and initialise
    any attributes they want persisted, then implement on_packet().
    """

    _EXCLUDED_STATE_ATTRS = {
        'config', 'state', 'portfolio', 'logger_factory',
        '_mongo_client', '_db', '_state_collection', 'strategy_id',
    }

    def __init__(self, strategy_id: str, config: dict):
        super().__init__(strategy_id, config)

        # Connect to state DB if STATUS_DB_URI is available
        status_uri = os.getenv('STATUS_DB_URI')
        self._mongo_client = None
        self._db = None
        self._state_collection = None
        if status_uri:
            try:
                self._mongo_client = MongoClient(status_uri, serverSelectionTimeoutMS=5000)
                self._db = self._mongo_client["status_manager"]
                self._state_collection = self._db["prev_state"]
            except Exception as e:
                logging.getLogger(__name__).warning(
                    f"[{strategy_id}] Could not connect to STATUS_DB_URI for state persistence: {e}"
                )

        self._initialize()

    def _initialize(self):
        """Initialization sequence. Called once during __init__."""
        self._setup_state_defaults()
        self._load_state_from_db()

    def _setup_state_defaults(self):
        """
        Override in child classes to declare state attributes and their defaults.
        This runs BEFORE DB load, so DB values will override any defaults set here.

        Example:
            self.regime = "neutral"
            self.max_pe_count = 0
        """
        pass

    def _get_state_attributes(self) -> List[str]:
        """Returns instance attribute names that should be persisted to/from DB."""
        return [
            attr for attr in self.__dict__.keys()
            if not attr.startswith('_') and attr not in self._EXCLUDED_STATE_ATTRS
        ]

    def _load_state_from_db(self):
        """Loads persisted state from MongoDB and sets matching instance attributes."""
        if self._state_collection is None:
            return
        try:
            doc = self._state_collection.find_one({"strategy_id": self.strategy_id})
            if doc:
                for attr in self._get_state_attributes():
                    if attr in doc:
                        setattr(self, attr, doc[attr])
                        self.log(f"Loaded state: {attr} = {doc[attr]}")
                self.log(f"State restored from DB for {self.strategy_id}")
            else:
                self.log(f"No prior state in DB for {self.strategy_id}, using defaults")
        except Exception as e:
            logging.getLogger(__name__).warning(
                f"[{self.strategy_id}] Failed to load state from DB: {e}"
            )

    def save_state_to_db(self):
        """
        Saves current instance state to MongoDB (upsert).
        Called by main.py at end of each collection when daily_mode is enabled.
        """
        if self._state_collection is None:
            return
        try:
            doc = {"strategy_id": self.strategy_id}
            for attr in self._get_state_attributes():
                if hasattr(self, attr):
                    doc[attr] = getattr(self, attr)
            self._state_collection.update_one(
                {"strategy_id": self.strategy_id},
                {"$set": doc},
                upsert=True,
            )
            self.log(f"State saved to DB: {doc}", level="DEBUG")
        except Exception as e:
            logging.getLogger(__name__).error(
                f"[{self.strategy_id}] Failed to save state to DB: {e}"
            )

    @abstractmethod
    def on_packet(self, packet: dict, execution_data: dict) -> List[Signal]:
        """
        Receives the raw nested packet from the DataProvider.
        Structure:
        {
            "timestamp": int,
            "timeframe": str,
            "current_data": { "BTC": {...}, "ETH": {...} }
        }

        Args:
            packet (dict): The market data packet.
            execution_data (dict): The pre-resolved execution map for the current tick.
                                 Format: { symbol: {c: price, ...} }
        """
        pass

    def on_trigger(self, data: dict, execution_data: dict) -> Optional[Union[Signal, List[Signal]]]:
        """Not used in Fast mode, but required by ABC."""
        return None
