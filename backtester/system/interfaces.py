"""
System Interfaces and Shared Data Structures
============================================
Defines the core data contracts and protocols to decouple 
implementation details (Portfolio/Strategies) from the execution engine.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, List, Protocol, runtime_checkable, Optional, Union
import polars as pl # type: ignore

@dataclass
class Position:
    """
    Represents a single, live, open trade with its live metadata and performance.
    Shared across all Portfolio implementations.
    """
    symbol: str
    trade_id: str
    quantity: int
    entry_price: float
    entry_timestamp: Any
    metadata: Dict[str, Any] = field(default_factory=dict)
    running_pnl_points: float = 0.0
    running_pnl_pct: float = 0.0
    last_known_price: float = 0.0
    last_update_timestamp: Any = None

    @property
    def direction(self) -> str:
        """Returns semantic trade direction: LONG or SHORT."""
        return "LONG" if self.quantity > 0 else "SHORT"

    @property
    def position_status(self) -> int:
        """Returns integer status: 1 for Long, -1 for Short (matches CSV output)."""
        return 1 if self.quantity > 0 else -1

@runtime_checkable
class IPortfolio(Protocol):
    """
    Interface Contract for Portfolio Management.
    Ensures that ExecutionEngine and Strategies can interact with *any* 
    portfolio implementation (Dict-based, DataFrame-based, etc.) generically.
    """
    
    @property
    def closed_trades_df(self) -> pl.DataFrame | None:
        ...

    def get_active_positions(self, strategy_id: str, symbol: Optional[str] = None) -> List[Position]:
        """Returns standard list of Position objects."""
        ...

    def get_active_positions_df(self, strategy_id: str, symbol: Optional[str] = None) -> pl.DataFrame | None:
        """Returns DataFrame view of open trades."""
        ...

    def get_closed_positions(self, strategy_id: str, symbol: Optional[str] = None) -> List[Dict[str, Any]]:
        """Returns list of closed trade dictionaries."""
        ...

    def get_closed_positions_df(self, strategy_id: str, symbol: Optional[str] = None) -> pl.DataFrame | None:
        """Returns DataFrame view of closed trades."""
        ...

    def open_position(
        self, 
        strategy_id: str, 
        symbol: str, 
        quantity: int, 
        price: float, 
        timestamp: Any, 
        metadata: Dict[str, Any]
    ) -> str:
        ...

    def close_position(
        self, 
        strategy_id: str, 
        symbol: str, 
        trade_id: str, 
        exit_price: float, 
        exit_timestamp: Any, 
        exit_reason: str = "UNKNOWN"
    ) -> Union[bool, None]:
        ...

    def liquidate_portfolio(self, final_prices: Dict[str, float], final_timestamp: Any):
        ...

    def mark_to_market(self, strategy_id: str, packet: Dict[str, Any], timestamp: Any):
        ...

    def persist_trade_history(self):
        ...

    def summarize_results(self):
        ...
