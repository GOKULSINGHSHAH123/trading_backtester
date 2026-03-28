"""
Base Strategy class (v6)
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any, List, Union, Callable
from collections import defaultdict
import logging
import polars as pl # type: ignore

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
    """
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
