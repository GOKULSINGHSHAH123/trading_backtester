# Changelog - v2.2 (Feature: Trade Log Validator)

This release introduces a production-grade **Trade Log Validator** (`trade_log_validator`), a self-contained module that strictly audits simulation outputs against historical market data and business rules. This ensures high confidence in backtest fidelity by detecting discrepancies in pricing, timestamps, and logic.

## Key Features

### 1. Integrated Validation Pipeline
*   **Automatic Invocation**: The `ArtifactManager` now automatically triggers validation at the end of every simulation run.
*   **Dual-Mode**: Supports `UNIVERSAL` mode (PnL, Time checks) and specialized `OPTIONS` mode (Expiry, Lot Size checks).
*   **Reporting**: Generates a detailed `violations.csv` and a machine-readable `validation_summary.json` in the run's `validation/` directory.

### 2. Robust Checks (`trade_log_validator/checks/`)
*   **Market Data**: Validates every Entry/Exit price against the Orb API's historical LTP (Last Traded Price) to detect slippage or data quality issues.
*   **Business Rules**: Enforces logical constraints:
    *   Exit Time > Entry Time
    *   Trading within Market Hours (09:15 - 15:25)
    *   PnL Consistency (Math checks)
*   **Data Integrity**: Flags nulls, zero prices, and negative quantities.
*   **Options Specific**: Validates that trades do not occur after contract expiry and that quantities match authorized lot sizes.

### 3. Architecture
*   **High Performance**: Built on `polars` for vectorized validation of large trade logs.
*   **Optimized API Client**: `OrbService` uses smart batching to fetch thousands of data points efficiently.
*   **Timezone Aware**: Strictly handles IST/UTC conversions to ensuring alignment between simulation timestamps and database records.

---

# Changelog - v2.1 Critical Fix: PnL Accounting & Liquidation Consistency

This update addresses a critical mathematical discrepancy between the Mark-to-Market (MTM) equity curve and the final Realized PnL summary. It introduces "Explicit Price Tracking" to ensure that force-closed positions at the end of a simulation use the exact same price reference as the final MTM snapshot, eliminating "vanishing profit" scenarios caused by data gaps or end-of-stream edge cases.

## A. Critical Fixes

### 1. Explicit Price Tracking (`system/interfaces.py`, `system/portfolio.py`)
*   **Problem**: The system relied on "implicit" state, calculating PnL on the fly. If a symbol stopped receiving price updates (e.g., end of data stream), the MTM curve would "freeze" at the last known profitable value. However, the final `liquidate_portfolio` call might fail to find a valid "current price" and fall back to closing the trade at its entry price (PnL = 0), causing realized profits to disappear from the final report.
*   **Solution**: Added `last_known_price` and `last_update_timestamp` attributes to the `Position` object.
*   **Mechanism**:
    *   `mark_to_market` now explicitly updates these fields on the `Position` object every time it processes a valid tick.
    *   `liquidate_portfolio` now uses `position.last_known_price` as the authoritative fallback if a fresh market price is unavailable.
*   **Result**: The final Realized PnL is mathematically guaranteed to match the final Unrealized PnL seen on the equity curve.

### 2. Post-Liquidation MTM Snapshot (`system/main.py`)
*   **Problem**: The MTM equity curve was recorded *before* the final liquidation event. This meant the chart ended at "Time T" (Unrealized State), while the Summary Report reflected "Time T + Epsilon" (Realized State).
*   **Solution**: Added an explicit `portfolio.store_mtm_snapshot()` call immediately after `liquidate_portfolio()` returns.
*   **Result**: The equity curve now correctly visually concludes at the final realized account value.

### 3. Stream Termination Logic (`system/main.py`)
*   **Problem**: If the final data packet in a stream contained no timestamp (e.g., a footer), the `last_timestamp` variable could be overwritten with `None`, causing the entire `liquidate_portfolio` block to be skipped.
*   **Solution**: Refactored the simulation loop to only update the `last_timestamp` tracking variable when a valid timestamp is present in the packet.

### 4. Robust CSV Flushing (`system/portfolio.py`)
*   **Problem**: `persist_trade_history` relied on `file.tell()` to detect if a header was needed, which can be unreliable in append mode on some filesystems, potentially corrupting the trade log.
*   **Solution**: Switched to `os.path.getsize()` to robustly check for file emptiness before writing headers.

### 5. Position Object Update (`system/interfaces.py`)
*   Added `last_known_price` (float) and `last_update_timestamp` (int [epoch]) to the `Position` dataclass.
*   These fields provide an explicit audit trail for the state of every open position, decoupling the "valuation" of a position from the transient "current market price" variable in the simulation loop.


## B. Portfolio Enhancements

Focused refactoring of the accounting engine (`system/portfolio.py`) to eliminate redundant calculations and optimize I/O during the simulation lifecycle.

### 1. Optimized MTM Snapshot
*   **Previous Behavior**: `store_mtm_snapshot` recalculated the PnL for every position by performing a fresh price lookup and arithmetic operation, duplicating the work already done by `mark_to_market` in the same tick loop.
*   **New Behavior**: `store_mtm_snapshot` now aggregates the pre-calculated `running_pnl_points` stored on the `Position` objects.
*   **Impact**:
    *   Removed O(N) redundant calculations per tick (where N = number of open positions).
    *   Reduced dictionary lookups and potential points of failure for missing price data.

### 2. Centralized Result Summarization
*   **Previous Behavior**: The `summarize_results` method re-read the `trades.csv` file from disk *separately* for every strategy to generate the JSON output, plus once more for the global summary.
*   **New Behavior**: The trade log is read from disk exactly once into a Polars DataFrame, processed, and then filtered in-memory for each strategy's report.
*   **Impact**:
    *   Reduced File I/O complexity from O(S) to O(1) (where S = number of strategies).
    *   Significantly faster report generation for simulations with many strategies or large trade logs.

---

# Changelog - v2.0 (Nested Packet Architecture)

This version introduces a significant performance and structural overhaul to the Data Provider and Simulation Engine, optimizing how multi-timeframe market data is processed and consumed by strategies.

## Major Enhancements

### 1. Nested Data Packets & Multi-Timeframe Optimization
*   **Previous Behavior**: The Data Provider emitted separate events for each timeframe (e.g., one packet for 1m, another for 5m), forcing the engine to loop multiple times per timestamp.
*   **New Behavior**: The Data Provider now emits a single **Nested Packet** per timestamp containing all available timeframe updates.
    *   Structure: `{ 'timestamp': 12345, '1m': {...}, '5m': {...} }`
*   **Impact**:
    *   Reduced function call overhead in the main loop.
    *   Enabled **Atomic Decision Making**: Fast strategies can now see the state of 1m, 5m, and 1h candles *simultaneously* in a single `on_packet` call.

*   **[Also] Execution Data**: the format for tick-data (`execution_data` in the framework) has been changed. now the strategy accesses tick data via just using the symbol for lookup.
    *   Structure: `{ 'sym1': {'c': ...}, 'sym2': {'c': ...} }`

### 2. High-Performance "Fast Strategy" Upgrades
*   **`BaseStrategyFast`** has been updated to receive the raw **Nested Packet**.
*   **Impact**: Strategies like Arbitrage or Options execution can now perform cross-timeframe logic (e.g., "Check 5m Trend AND 1m Entry") without needing internal state caching.
*   **Migration**: Existing Fast strategies (`nifty_options_strategy_fast.py`, `multi_tf_ema_fast.py`) have been refactored to iterate through the nested dictionary keys instead of assuming a flat structure.

### 3. "Adapter Pattern" for Standard Strategies
*   **Backward Compatibility**: To prevent breaking changes for standard trend-following strategies (`BaseStrategy`), an **Adapter Layer** was implemented in `system/main.py`.
*   **Mechanism**: The main loop automatically "unrolls" the new nested packet into individual `(symbol, timeframe)` events.
*   **Result**: Existing standard strategies (`rsi_ema_mtf.py`) continue to function **without any code changes**, receiving the same flat data structure they expect.

### 4. Reproducibility: Strategy Source Snapshot
*   **Feature**: The `ArtifactManager` now automatically copies the Python source code (`.py` files) of all strategies used in a simulation run.
*   **Location**: Source files are saved to `backtest_results/{Strategy}/{RunID}/inputs/strategies/`.
*   **Benefit**: Guarantees that the exact logic used to generate a backtest is preserved, even if the source code in the repository is modified later.

## 🛠 Refactoring Details

### System Core (`system/`)
*   **`artifact_manager.py`**:
    *   Updated `snapshot_inputs` to parse strategy configs and capture source code.
*   **`main.py`**:
    *   Refactored `run_simulation` loop to parse the new nested packet structure.
*   **`data_provider.py`**:
    *   Now aggregates all timeframes for a given timestamp into a single yield.

### Strategies (`strategies/`)
*   **`multi_tf_ema_fast.py`**: Updated to iterate through all keys in the nested packet to process multiple EMA signals concurrently.
*   **`nifty_options_strategy_fast.py`**: Refactored to explicitly extract `'5m'` data for signals while using spot prices from any available timeframe for risk management.
*   **`pair_trader_fast.py`**: Updated to target `'1m'` data within the nested structure.
*   **`templates/template_strategy_fast.py`**: Updated documentation and example code to reflect the new API.

## Documentation
*   **`README.md`**:
    *   Updated "System Architecture" to describe the new data flow.
    *   Rewrote `BaseStrategyFast` guide to explain the Nested Packet structure.
    *   Added clarification on the "Adapter" behavior for Standard Strategies.

---

# Changelog - v1.5 (Refactor: Unify Portfolio Interface)

This major refactor aligns the framework's terminology with standard financial domain modeling and simplifies the developer API. The core distinction between "Actions" (Trades) and "State" (Positions) is now enforced throughout the system. The `BaseStrategy` API has been unified to provide a single, polymorphic entry point for retrieving position data.

## 1. Interface & Portfolio Terminology (`system/interfaces.py`, `system/portfolio.py`)
We have renamed methods to reflect the *business process* rather than the implementation detail.

| Old Method Name | **New Method Name** | Rationale |
| :--- | :--- | :--- |
| `get_open_trades` | **`get_active_positions`** | Returns `Position` objects, representing current holding state. |
| `get_open_trades_df` | **`get_active_positions_df`** | Consistency with above. |
| `open_trade` | **`open_position`** | Creating a new holding. |
| `close_trade` | **`close_position`** | Terminating a holding. |
| `close_all_open_positions` | **`liquidate_portfolio`** | Standard financial term for immediate closure of all assets. |
| `update_pnl_bulk` | **`mark_to_market`** | Describes the financial process of updating asset values. |
| `flush_closed_trades_to_csv` | **`persist_trade_history`** | Decouples interface from storage medium (CSV). |

## 2. Strategy API Unification (`system/strategy.py`)
The developer experience has been streamlined by merging disparate retrieval methods into a single, intuitive API.

*   **Unified `get_positions(symbol=None)`**: 
    *   **Old Behavior**: `get_positions(symbol)` required a symbol argument.
    *   **New Behavior**: `get_positions(symbol=None)` is polymorphic.
        *   If `symbol` is provided: Returns positions for that specific symbol.
        *   If `symbol` is `None`: Returns **all** active positions for the strategy (replaces the need for a separate `get_open_positions` method).
*   **Removed**: `get_open_positions()` (Redundant).
*   **Updated**: `get_positions_df()` now calls `get_active_positions_df` internally.

## 3. Core Engine Updates
*   **`system/execution_engine.py`**: Updated to call `open_position` and `close_position`.
*   **`system/main.py`**: Updated simulation loop to use `mark_to_market`, `liquidate_portfolio`, and `persist_trade_history`.

## 4. Strategy Compatibility
*   **`strategies/grid_trader_fast.py`**: Updated direct portfolio call from `update_pnl_bulk` to `mark_to_market`.
*   **`strategies/rsi_ema_mtf.py`**: Updated to use `get_active_positions`.
*   **Templates**: Updated `template_strategy.py` and `template_strategy_fast.py` to reflect the new API best practices.

## 5. Documentation
*   **`README.md`**: rewritten to document the new API methods, updated examples, and removed references to the deprecated `portfolio_p` (Polars backend) which was removed in a previous cleanup.

