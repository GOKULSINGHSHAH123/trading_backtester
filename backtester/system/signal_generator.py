"""
Signal Orchestrator class
"""

import logging
from typing import Dict, List
from collections import defaultdict

from backtester.system.strategy import BaseStrategy, Signal
from backtester.system.interfaces import IPortfolio


class SignalOrchestrator:
    def __init__(self):
        self.strategies: Dict[str, BaseStrategy] = {}
        # The key is now a tuple: (symbol, timeframe)
        self.routing_map: Dict[tuple[str, str], List[str]] = defaultdict(list)
        self.logger = logging.getLogger(__name__)

    def add_strategy(self, strategy_id: str, strategy_inst: BaseStrategy):
        self.strategies[strategy_id] = strategy_inst
        
        # Read the new, more descriptive configuration
        instrument_timeframes = strategy_inst.config.get("instrument_timeframes", {})
        
        for symbol, timeframes in instrument_timeframes.items():
            for timeframe in timeframes:
                self.routing_map[(symbol, timeframe)].append(strategy_id)
                self.logger.info(f"[ORCHESTRATOR] Subscribed '{strategy_id}' to '{symbol}' on timeframe '{timeframe}'")

    def route_market_data(
        self, flat_packet: dict, execution_data: dict
    ) -> List[Signal]:
        """
        Takes a pre-compiled, flat data packet for a single symbol, finds
        subscribed strategies based on symbol AND timeframe, and dispatches.

        Args:
            flat_packet (dict): Compiled data for a single symbol.
            execution_data (dict): Pre-resolved execution map for the current tick.
                                 Format: { symbol: {c: price, ...} }
        """
        signals = []
        symbol = flat_packet["symbol"]
        timeframe = flat_packet["timeframe"]

        # Precise lookup using both symbol and timeframe
        subscriber_ids = self.routing_map.get((symbol, timeframe), [])
        if not subscriber_ids:
            return []

        for strat_id in subscriber_ids:
            strategy = self.strategies[strat_id]
            # All exit logic is responsibility of the developer.
            new_signals = strategy.on_trigger(flat_packet, execution_data)
            
            if new_signals:
                if isinstance(new_signals, list):
                    signals.extend(new_signals)
                else:
                    signals.append(new_signals)

        return signals
