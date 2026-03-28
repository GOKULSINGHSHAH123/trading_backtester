"""
Simulation Engine Core
====================
This module contains the primary `run_simulation` function, which orchestrates
the entire backtesting process. It is designed to be called by an external
runner script after all configurations have been loaded.
"""

import time
import logging
from typing import Dict, List, Any

from backtester.system.execution_engine import ExecutionEngine
from backtester.system.signal_generator import SignalOrchestrator
from backtester.system.strategy import BaseStrategy, BaseStrategyFast

# Initialize module logger
logger = logging.getLogger(__name__)

def run_simulation(data_config: Dict[str, Any], strategies: List[BaseStrategy], provider_class: Any, portfolio_class: Any):

    """
    Main simulation orchestrator.
    Integrates DataProvider with strategy execution pipeline.
    """

    logger.info("=" * 60)
    logger.info("CONFIG-DRIVEN TRADING SIMULATION SYSTEM")
    logger.info("=" * 60)
    logger.info("\n[INIT] Initializing components...")

    

    # Dependency Injection: Instantiate components
    portfolio = portfolio_class()
    orchestrator = SignalOrchestrator()
    execution_engine = ExecutionEngine(portfolio)


    # Separate strategies and inject dependencies
    fast_strategies = []
    standard_strategies = []

    for strategy in strategies:
        strategy.set_portfolio(portfolio)

        if isinstance(strategy, BaseStrategyFast):
            fast_strategies.append(strategy)
        else:
            standard_strategies.append(strategy)
        
        orchestrator.add_strategy(strategy.strategy_id, strategy)



    logger.info(f"[INIT] Strategy Mode: Mixed ({len(fast_strategies)} Fast, {len(standard_strategies)} Standard)")
    logger.info("\n[INIT] Initializing DataProvider...")

    with provider_class(data_config) as provider:
        # Register strategy-defined data transformations
        for strategy in strategies:
            # 1. Per-Instrument Transform
            if strategy.get_data_transform:
                provider.register_transform(strategy.get_data_transform)
                logger.info(f"   + Registered data transform from strategy: {strategy.strategy_id}")

            
            # 2. Global Batch Transform
            global_transform_func = getattr(strategy, "get_global_transform", None)
            if global_transform_func and global_transform_func != BaseStrategy.get_global_transform:
                 provider.register_global_transform(global_transform_func)
                 logger.info(f"   + Registered GLOBAL data transform from strategy: {strategy.strategy_id}")


        collection_numbers = provider.get_collection_numbers()
        logger.info(f"[INIT] Will process {len(collection_numbers)} collection(s): {collection_numbers}")

        
        # Performance tracking
        time_routing = 0
        time_execution = 0
        total_packets = 0
        signal_count = 0
        last_timestamp = None
        current_prices = {}
        overall_start_time = time.time()

        
        for collection_num in collection_numbers:

            logger.info(f"\n{'='*60}")
            logger.info(f"[COLLECTION {collection_num}] Starting processing...")
            logger.info(f"{'='*60}")

            provider.initialize(collection_num)
            logger.info(f"\n[SIM] Streaming from collection {collection_num}...")

            for nested_packet, execution_data in provider.stream():
                total_packets += 1
                current_ts = nested_packet.get("timestamp", None)
                if not current_ts:
                    continue
                
                # Only update the tracking variable if we have a valid timestamp
                last_timestamp = current_ts

                # Update final prices from current execution packet
                current_tick_execution_data = execution_data.get(str(last_timestamp), {})

                if current_tick_execution_data:
                    for symbol, price_data in current_tick_execution_data.items():
                        if "c" in price_data:
                            current_prices[symbol] = price_data["c"]

                # --- IMPLICIT PnL UPDATE ---
                for strategy in strategies:
                    portfolio.mark_to_market(strategy.strategy_id, current_prices, last_timestamp)
                    
                signals = []
                # --- FAST PATH ROUTING ---
                if fast_strategies:
                    t1 = time.perf_counter()
                    for f_strat in fast_strategies:
                        # Fast strategies always return a list per contract
                        signals.extend(
                            f_strat.on_packet(nested_packet, current_tick_execution_data)
                        )

                    t2 = time.perf_counter()
                    time_routing += t2 - t1

                # --- STANDARD PATH ROUTING ---
                if standard_strategies:
                    t1 = time.perf_counter()
                    
                    # Iterate through all timeframes in the nested packet
                    for key, tf_data in nested_packet.items():
                        if key == 'timestamp':
                            continue
                        
                        timeframe = key
                        # tf_data structure: {'current_data': {SYM: {...}}}
                        current_data = tf_data.get("current_data", {})
                        
                        for symbol, data in current_data.items():
                            flat_packet = {
                                "symbol": symbol,
                                "timeframe": timeframe,
                                "timestamp": last_timestamp,
                                **data,
                            }
                            signals.extend(
                                orchestrator.route_market_data(
                                    flat_packet, current_tick_execution_data
                                )
                            )

                    t2 = time.perf_counter()
                    time_routing += t2 - t1

                # --- UNIFIED EXECUTION ---
                if signals:
                    signal_count += len(signals)
                    t3 = time.perf_counter()
                    for signal in signals:
                        execution_engine.execute(signal, current_tick_execution_data)
                    t4 = time.perf_counter()
                    time_execution += t4 - t3

                # --- IMPLICIT MTM SNAPSHOT ---
                for strategy in strategies:
                    portfolio.store_mtm_snapshot(strategy.strategy_id, current_prices, last_timestamp)

            logger.info(f"\n[COLLECTION {collection_num}] Completed.")

            daily_mode = data_config.get('daily_mode', False)

            # Finalize the completed backtest for this collection
            logger.info(f"[COLLECTION {collection_num}] Finalizing trades...")
            if last_timestamp is not None:
                if daily_mode:
                    # Daily mode: persist open positions to MongoDB instead of liquidating
                    logger.info("[DAILY] Capturing final MTM snapshot...")
                    for strategy in strategies:
                        portfolio.store_mtm_snapshot(strategy.strategy_id, current_prices, last_timestamp)

                    logger.info("[DAILY] Saving open positions to MongoDB...")
                    portfolio.save_all_open_positions_to_db()

                    logger.info("[DAILY] Saving strategy states to MongoDB...")
                    for strategy in strategies:
                        if hasattr(strategy, 'save_state_to_db'):
                            try:
                                strategy.save_state_to_db()
                                logger.info(f"   ✓ State saved for {strategy.strategy_id}")
                            except Exception as e:
                                logger.error(f"   ✗ Failed to save state for {strategy.strategy_id}: {e}")
                else:
                    # Historical mode: liquidate all open positions at end of run
                    logger.info(f"[LIQ] Triggering portfolio liquidation at TS: {last_timestamp}")
                    portfolio.liquidate_portfolio(current_prices, last_timestamp)

                    logger.info("[LIQ] Capturing post-liquidation snapshot...")
                    for strategy in strategies:
                        portfolio.store_mtm_snapshot(strategy.strategy_id, current_prices, last_timestamp)
            else:
                logger.error(f"[EXCEPTION] No timestamps processed for collection {collection_num}. Skipping finalization.")

            # Flush the completed collection's trades to disk
            portfolio.persist_trade_history()

            # Clear memory for the next run
            logger.info(f"[COLLECTION {collection_num}] Clearing memory...")
            for strategy in strategies:
                if hasattr(strategy, 'state'):
                    strategy.state.clear()
            provider.clear_memory()

        
        overall_end_time = time.time()
        total_time = overall_end_time - overall_start_time if overall_end_time > overall_start_time else 1

        
        logger.info(f"\n[SIM] All collections processed in {total_time:.2f}s")
        provider.print_summary() 

        logger.info("\n" + "=" * 60)
        logger.info("PERFORMANCE BREAKDOWN")
        logger.info("=" * 60)
        logger.info(f"Total Packets Processed:        {total_packets:,}")
        logger.info(f"Total Signals Generated:        {signal_count:,}")
        logger.info(f"Total Execution Time:           {total_time:.4f}s")

        if total_time > 0:
            logger.info(f"  - Signal Generation (Routing): {time_routing:.4f}s ({time_routing/total_time*100:.1f}%)")
            logger.info(f"  - Trade Execution:            {time_execution:.4f}s ({time_execution/total_time*100:.1f}%)")

        if total_packets > 0 and total_time > 0:
            logger.info(f"Throughput:                     {total_packets/total_time:.0f} packets/sec")

        # --- Final Summary ---
        logger.info("\n[FINAL] Generating final portfolio summary...")
        portfolio.summarize_results()

    logger.info("\n" + "=" * 60)
    logger.info("SIMULATION COMPLETE")
    logger.info("=" * 60)
