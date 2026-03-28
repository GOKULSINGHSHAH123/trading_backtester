"""
Execution Engine Module
"""
import logging
from backtester.system.interfaces import IPortfolio
from backtester.system.strategy import Signal

class ExecutionEngine:
    def __init__(self, portfolio: IPortfolio):
        self.portfolio = portfolio
        self.logger = logging.getLogger(__name__)

    def execute(self, signal: Signal, execution_data: dict):
        """Processes a signal and updates the portfolio state."""
        price = 0.0

        # Attempt to find a valid execution price from the current tick's data
        symbol_data = execution_data.get(signal.symbol)
        if symbol_data and "c" in symbol_data:
            price = symbol_data["c"]

        # If a valid, positive price was not found, skip the trade and log a warning.
        if price < 0.0:
            self.logger.warning(f"[EXECUTION] Signal for {signal.symbol} at {signal.timestamp} skipped. No valid execution price found in execution data.")
            return

        # --- If we get here, price is valid and we can execute ---

        if signal.action == "BUY" or signal.action == "SELL":
            self.portfolio.open_position(
                strategy_id=signal.strategy_id,
                symbol=signal.symbol,
                quantity=signal.quantity if signal.action == "BUY" else -signal.quantity,
                price=price,
                timestamp=signal.timestamp,
                metadata=signal.metadata
            )
        elif signal.action == "EXIT":
            if not signal.trade_id_to_close:
                self.logger.error(f"EXIT signal for {signal.symbol} is missing a trade_id_to_close.")
                return
            self.portfolio.close_position(
                strategy_id=signal.strategy_id,
                symbol=signal.symbol,
                trade_id=signal.trade_id_to_close,
                exit_price=price,
                exit_timestamp=signal.timestamp,
                exit_reason=signal.metadata.get("reason", "UNKNOWN")
            )
