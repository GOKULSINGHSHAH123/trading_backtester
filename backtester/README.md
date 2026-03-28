# Backtesting Framework

This is a high-performance, event-driven backtesting engine designed for institutional-grade quantitative trading. It leverages Polars for vectorized data processing and Multiprocessing for parallelized indicator computation, enabling the simulation of complex multi-asset strategies over high-frequency data with minimal latency.

The system is architected to eliminate lookahead bias through a strict chronological event loop and supports swappable components via dependency injection.

### Key Features
*   **Event-Driven Execution**: Simulates live market behavior; strategies react to "packets" of data step-by-step.
*   **Hybrid Architecture**: Supports both **Standard** (symbol-level abstraction) and **Fast** (raw packet access) strategy implementations.
*   **Parallel Data Pipeline**: Indicators are pre-calculated in parallel processes using `get_data_transform` before the simulation loop begins.
*   **Optimized Portfolio**: O(1) Dictionary-based backend with cached Polars views for high-speed access and complex analysis.
*   **Cross-Asset Capable**: Native support for multi-leg options strategies, grid trading, and basket trading.
*   **Strategy & Symbol Logging**: Isolated logging context for each strategy (`{strategy_id}.log`) and optionally per symbol (`{symbol}_{strategy_id}.log`) to debug logic without cluttering the main execution log.

---

## Quick Start

### 1. Prerequisites

**Step 1: Create and Activate Virtual Environment**
It is strictly recommended to run the framework inside an isolated virtual environment.

*Unix/macOS:*
```bash
python3 -m venv .venv
source .venv/bin/activate
```

*Windows:*
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

**Step 2: Install Dependencies**
```bash
pip install -r requirements.txt
```

### 2. Environment Setup
Create a `.env` file in the project root to configure connectivity. [ **Note**: use `.env.example` ]

```bash
# Destination MongoDB (Where optimized backtest data will be stored)
DEST_DB_URI="--the destinationDB URI where you aim to store data--"

# Source Data Layer (Orb DAL Credentials)
ORB_API_URL="--orb_api_url--"
ORB_CLIENT_ID="username"
ORB_CLIENT_SECRET="api_key"
```

### 3. Data Migration (ETL)
**Crucial:** The backtester runs on a local, optimized copy of the market data. You must migrate data from the source (Orb DAL) to your destination DB before running simulations.

1.  **Configure Scope**: Edit `configs/migration_config.yml` to define the Date Range (`starting_date`, `ending_date`), Instruments (`stocks`, `index_options`), and Data Fields (`projections`).
    ```bash
    python scripts/run_migration.py
    ```
    *See `scripts/README.md` for detailed migration instructions.*

### 4. Simulation Configuration
The simulation runtime is driven by `simulation_config.yml`. (use `simulation_config.yml.example`)use This master file links the portfolio backend, strategies, and the specific dataset to use. It also defines the names for organizing your backtest results.

```yaml
# --- Experiment Tracking ---
experiment_name: "Pair_Trading_Reversion_Analysis"
run_tag: "tight_sl_v1" # This tag will be part of the folder name for this run's artifacts
```

**Note:** Ensure `db_name` in `simulation_config.yml` matches the `db_name` you used in `configs/migration_config.yml`.

### 5. Running a Simulation
Execute the runner script, pointing it to your master configuration file.

```bash
python run_simulation.py
```

The simulation will:
1.  Create a unique, timestamped artifact folder under `backtest_results/{experiment_name}/`.
2.  Snapshot your config files and Git state into the artifact folder.
3.  Load data from your local DB and compute indicators.
4.  Stream market events to your strategies, logging all output to a file within the artifact folder.
5.  Execute trades and track PnL.
6.  Save `trades.csv` and `output.json` (Combined MTM & Trades) into the artifact folder `/outputs`.
7.  (Optional) Auto-validate the trade log against the Orb platform.
    7.1 the results (`validation.log` and `violations.csv`) will be saved in `/validation`
---

## System Architecture

The simulation pipeline consists of five decoupled layers:

1.  **Data Provider (`system.data_provider`)**:
    *   Fetches raw tick/OHLCV data (MongoDB/CSV).
    *   **Dynamic Resampling**: Automatically converts any requested timeframe string (e.g., "3m", "2h", "1d") into seconds for resampling.
    *   **Dynamic Projection**: Capable of handling arbitrary data fields defined in the migration config (not just standard OHLCV).
    *   **Crucial:** Executes strategy-defined `get_data_transform` methods in parallel to inject indicators (RSI, EMA, etc.) into the dataframe *before* the loop starts.
    *   **Advanced:** Supports `get_global_transform` for cross-asset calculations (e.g., Spreads, Ratios) on the aggregated dataset.

2.  **Signal Orchestrator (`system.signal_generator`)**:
    *   Routes market data packets to subscribed strategies.
    *   Manages multi-timeframe synchronization.

3.  **Strategy Layer (`strategies/`)**:
    *   Receives market data and Portfolio state.
    *   Returns `Signal` objects (BUY, SELL, EXIT).

4.  **Execution Engine (`system.execution_engine`)**:
    *   Validates signals against `execution_data` (high-fidelity pricing).
    *   Fills orders and updates the Portfolio.

5.  **Portfolio (`system.portfolio`)**:
    *   The "Source of Truth" for all open positions and closed trade logs.

6.  **Trade Log Validator (`trade_log_validator/`)**:
    *   **Post-Processing**: Audits the final `trades.csv` against external market data (Orb API).
    *   **Compliance**: Enforces business rules (Market Hours, Lot Sizes) and Data Integrity.
    *   **Reporting**: Generates a detailed `violations.csv` report.

---

## Configuration Guide

### 1. Master Configuration (`simulation_config.yml`)
This is the single source of truth for the simulation run.

```yaml
# --- Validation Configuration ---
# Whether to run the trade log validator after the simulation
validate_results: true
# If true, validates as "OPTIONS" segment (includes expiry checks, etc.). Else "UNIVERSAL".
is_options_segment: true

enable_trade_aggregation: true

# --- Database Configuration ---
db_name: "db_name"

# --- Simulation Scope ---
starting_ti_year: "01-01-2024"
ending_ti_year: "28-12-2024"

# Timeframes to resample from raw data
# Supports dynamic formats: '1m', '5m', '15m', '1h', '4h', '1d', etc.
timeframes: ["1m", "5m"]

# Universe definition
instruments: 
  - 'NIFTY 50'
  - 'RELIANCE'

# --- System Components ---

# List of strategies to instantiate for this run
strategy_configs:
  - "strategies/configs/grid_trader_fast_1.yml"
  - "strategies/configs/nifty_options_strategy_fast.yml"
```

### 2. Strategy Configuration (`strategies/configs/*.yml`)
Every strategy requires a YAML config that defines its class path and parameters.

```yaml
strategy_id: "MY_STRATEGY_001"
# Python path to the class
strategy_path: "strategies.my_strategy.MyStrategyClass"

# "all" to use all instruments from Data Provider, or a specific list
instrument_source: all 

# Timeframes this strategy needs (must exist in Data Provider config)
timeframes: ["1m", "5m"]

# Custom parameters passed to __init__
parameters:
  stop_loss: 0.05
  quantity: 10
```

---

## Strategy Development Guide

The framework supports two types of strategies. Choose the one that fits your latency and complexity requirements.


### Type A : `BaseStrategyFast`
Best for: **Arbitrage, HFT, Options/Derivatives, Grid Trading.**

Inherit from `system.strategy.BaseStrategyFast`.

**Key Differences:**
*   Receives the **entire multi-timeframe packet** (`on_packet`) for a timestamp.
*   **Packet Structure**: `{ 'timestamp': <int>, '1m': {...}, '5m': {...} }`.
*   Must handle its own iteration over timeframes and symbols.
*   Ideal for checking correlations between instruments across timeframes (e.g., 5m Trend vs 1m Entry).

**Key Methods:**
1.  **`get_data_transform(df, timeframe)` (Static)**:
    *   **Input**: Raw Polars DataFrame of OHLCV data.
    *   **Output**: DataFrame with added indicator columns (RSI, SMA, etc.).
    *   *Note*: This runs **once** at startup, in parallel processes. Do not put loop logic here.
2.  **`on_packet(packet, execution_data)`**:
    *   `packet`: Dict containing `timestamp` and keys for each available timeframe (e.g., `'1m'`, `'5m'`).
    *   `execution_data`: Pre-resolved map `{symbol: {c: price}}` for high-fidelity execution at the current tick.
    *   **Returns**: `List[Signal]`.

**Example:**
```python
class MyFastStrategy(BaseStrategyFast):
    def on_packet(self, packet, execution_data):
        # 1. Access specific timeframe data
        tf_1m = packet.get('1m', {}).get('current_data', {})
        tf_5m = packet.get('5m', {}).get('current_data', {})
        
        # 2. Access Spot Price (from 1m data)
        spot = tf_1m.get('NIFTY 50')
        option_symbol = 'NIFTY24JAN21500CE'
        
        # 3. Access Execution Price
        option_exec = execution_data.get(option_symbol)
        
        if spot and option_exec and spot['c'] > 21500:
             self.log(f"Spot crossed 21500: {spot['c']}")
             return self.create_signal(option_symbol, "BUY", 50, packet['timestamp'], "CROSSOVER", option_exec['c'])
```

### Type B: `BaseStrategy` 
Best for: **Trend Following, Multi-Timeframe, Indicator-based logic.**

Inherit from `system.strategy.BaseStrategy`.

**Note:** The system automatically "unrolls" the multi-timeframe packet into individual events for this strategy type. You receive one call per symbol, per timeframe.
1.  **`get_data_transform(df, timeframe)` (Static)**:
    *   **Input**: Raw Polars DataFrame of OHLCV data.
    *   **Output**: DataFrame with added indicator columns (RSI, SMA, etc.).
    *   *Note*: This runs **once** at startup, in parallel processes. Do not put loop logic here.

2.  **`on_trigger(data, execution_data)`**:
    *   Called **per symbol** when a new data point arrives.
    *   `data`: Dict containing `symbol`, `c` (close), `timestamp`, and your calculated indicators.
    *   `execution_data`: Pre-resolved map `{symbol: {c: price}}` for high-fidelity execution at the current tick.
    *   **Returns**: `Signal` or `List[Signal]` or `None`.
    *   **Accessing Open Positions**: Use `self.get_positions(symbol)`.

**Example:**
```python
class MyStrategy(BaseStrategy):
    @staticmethod
    def get_data_transform(df: pl.DataFrame, timeframe: str) -> pl.DataFrame:
        # Calculate EMA using Polars
        return df.with_columns(pl.col("c").ewm_mean(span=20).alias("EMA_20"))

    def on_trigger(self, data, execution_data):
        # Fetch open positions manually if needed
        open_positions = self.get_positions(data['symbol'])
        
        if data['c'] > data['EMA_20'] and not open_positions:
            self.log(f"Entry Signal for {data['symbol']}")
            return self.create_signal(data['symbol'], "BUY", 10, data['timestamp'], "EMA_CROSS", data['c'])
```

### Strategy Logging (`self.log`)

The framework provides isolated, persistent logging per strategy and per symbol. Unlike a standard `print()` statement, `self.log()` writes structured data to artifact files within the run directory, ensuring your console stays clean while your data remains preserved.

#### Storage Location

Logs are stored within the backtest results directory:
`backtest_results/.../logs/`

* **`{strategy_id}.log`**: Contains strategy-level execution details.
* **`{symbol}_{strategy_id}.log`**: Contains symbol-scoped execution details.

> `strategy.log` has been deprecated and removed in favor of these specific identifiers.

---

#### Routing Rules

To ensure clean isolation when running multiple strategies or symbols concurrently, the following logic is applied:

| Scenario | Destination | Duplicated to Strategy Log? |
| --- | --- | --- |
| `symbol` is provided | `{symbol}_{strategy_id}.log` | **No** |
| `symbol` is omitted | `{strategy_id}.log` | **N/A** |

* **No Propagation**: Symbol-scoped logs do **not** appear in the strategy-level log.
* **Zero Duplication**: Each log entry has exactly one destination.

---

#### Why Use It?

* **Isolation**: Keep strategy logic separate from engine or system-level logs.
* **Per-Symbol Debugging**: Diagnose behavior for a specific asset without wading through noise from other symbols.
* **Concurrency Safety**: Prevents log interleaving when multiple strategies run simultaneously.
* **Persistence**: Artifacts are saved automatically for post-run inspection and auditing.

---

#### API Reference

```python
def log(self, message: str, symbol: Optional[str] = None, level: str = "INFO")
```

**Log Levels:**

* **`INFO` (Default)**: Standard events like entries, exits, or state changes.
* **`WARNING`**: Non-critical anomalies that the strategy has handled.
* **`ERROR`**: Critical strategy-level failures.
* **`DEBUG`**: Highly verbose output for internal state inspection.

---

#### Examples

**1. Strategy-Level Log**
Used for portfolio-wide updates or general logic.

```python
self.log(f"Portfolio exposure updated to {exposure:.2f}")

```

* **Output (`{strategy_id}.log`):** `2024-01-01 10:00:00,123 Portfolio exposure updated to 0.35`

**2. Symbol-Scoped Log**
Used for asset-specific triggers.

```python
self.log(f"Entry signal at {price}", symbol="NIFTY 50")

```

* **Output (`NIFTY50_STRATEGY_ID.log`):** `2024-01-01 10:00:00,123 Entry signal at 21500`

**3. Debug-Level Log**
Used for deep-dive diagnostics.

```python
self.log(
    f"RSI: {rsi_val:.2f} | Threshold: {self.oversold_level}",
    symbol="BANKNIFTY",
    level="DEBUG"
)
```
---

### The Position Object

When you retrieve open positions via `self.get_positions()`, you receive a list of `Position` objects. These objects contain the following helper properties to make your logic more readable:

| Property | Type | Description |
| :--- | :--- | :--- |
| **`.direction`** | `str` | Returns `"LONG"` if quantity > 0, else `"SHORT"`. |
| **`.position_status`** | `int` | Returns `1` for Long, `-1` for Short (aligns with CSV output). |
| **`.quantity`** | `int` | The raw signed quantity (Positive = Long, Negative = Short). |
| **`.entry_price`** | `float` | The price at which the trade was filled. |
| **`.metadata`** | `dict` | Custom data passed during `create_signal`. |
| **`.last_known_price`** | `float` | The most recent market price used for MTM calculations. |
| **`.last_update_timestamp`** | `int` | The timestamp of the last price update. (epoch) |

**Example Usage:**
```python
for position in open_positions:
    if position.direction == "LONG":
        self.log(f"Monitoring Long Position for {position.symbol}")
    else:
        self.log(f"Monitoring Short Position for {position.symbol}")
```

---
### Signal Generation (`create_signal`)

All strategies must use the `self.create_signal()` method to generate instructions for the execution engine. This method ensures that the signal is correctly mapped to your strategy and that the trade log remains audit-ready.

**Method Signature:**
```python
def create_signal(
    self,
    symbol: str,
    action: str,            # "BUY", "SELL", or "EXIT"
    quantity: int,
    timestamp: datetime,
    reason: str,            # Logged as ExitType in trades.csv
    price: float,           # The trigger price for audit purposes
    custom_metadata: dict,  # (Optional) Extra trade data
    trade_id_to_close: str  # (Required for EXIT) The ID of the trade to close
) -> Signal
```

**Key Parameters:**
*   **`reason`**: This string is critical for post-trade analysis. It appears in the `ExitType` column of your results, allowing you to filter performance by trigger logic (e.g., `"STOP_LOSS"` vs `"SIGNAL_FLIP"`).
*   **`price`**: This represents the price at the time of the signal. Note that in a backtest, the actual fill price is determined by the `ExecutionEngine` based on `execution_data`.
*   **`trade_id_to_close`**: When the action is `"EXIT"`, you **must** provide the unique ID of the trade you are closing. This allows the engine to support multi-leg and partial exit strategies.

**Example Usage:**

```python
# 1. Opening a position
return self.create_signal(
    symbol=data['symbol'],
    action="BUY",
    quantity=100,
    timestamp=data['timestamp'],
    reason="EMA_CROSSOVER",
    price=data['c']
)

# 2. Exiting a specific position
for position in open_positions:
    if data['c'] >= position.metadata['target']:
        return self.create_signal(
            symbol=position.symbol,
            action="EXIT",
            quantity=position.quantity,
            timestamp=data['timestamp'],
            reason="TAKE_PROFIT",
            price=data['c'],
            trade_id_to_close=position.trade_id
        )
```
---

### Global Transforms
Best for: **Pair Trading, Basket Trading, Index Arbitrage.**

If your strategy requires visibility of **multiple assets simultaneously** to calculate indicators (e.g., `Price(A) / Price(B)`), standard `get_data_transform` will not work because it runs in isolated processes.

Use `get_global_transform(data_map)` instead. 
**NOTE: This method can be used in both BaseStrategy and BaseStrategyFast.**

**Method Signature:**
*   **Input**: `Dict[Symbol, Dict[Timeframe, pl.DataFrame]]` - The entire loaded dataset for the current batch.
*   **Output**: `Dict` - The modified dataset (you can inject new columns or new synthetic symbols).
*   **Execution**: Runs **sequentially** in the main process after parallel loading completes but before streaming starts.

**Performance Warning:** 
This method pauses the pipeline. Heavy computations here effectively single-thread your data loading. Ensure your logic is fully vectorized using Polars.

**Example:**
```python
    @staticmethod
    def get_global_transform(data_map: Dict[str, Dict[str, pl.DataFrame]]) -> Dict[str, Dict[str, pl.DataFrame]]:
        # Calculate Ratio between TCS and INFY
        df_tcs = data_map['TCS']['1m']
        df_infy = data_map['INFY']['1m']
        
        # Join and Calculate
        combined = df_tcs.join(df_infy, on='ti', suffix='_infy')
        combined = combined.with_columns((pl.col('c') / pl.col('c_infy')).alias('ratio_TCS_INFY'))
        
        # Inject back into Primary Symbol
        data_map['TCS']['1m'] = combined
        return data_map
```


---

## Portfolio System

The system uses a highly optimized **Standard Backend** (`system.portfolio.Portfolio`) that combines the speed of O(1) dictionary lookups with the analytical power of Polars DataFrames via caching.

**Design Principle:** Strategies should **not** interact with the portfolio object directly. Instead, they should use the helper methods provided by `BaseStrategy` to ensure type safety and abstraction.

**Underlying Storage:** Nested Dictionary (`Dict[StrategyID, Dict[Symbol, Dict[TradeID, Position]]]`).

*   **Primary Use Case:** High-Frequency Trading, Grid Trading, strategies iterating over specific positions.
*   **Performance:**
    *   **O(1)** for adding/removing positions and retrieving by ID.
    *   **O(1)** for `get_active_positions_df` calls within the same tick (Cached).
*   **Metadata**: Flexible. Stores any JSON-serializable dictionary.

**Recommended Access Pattern:**
Use `self.get_positions()` to retrieve a list of `Position` objects.

```python
# FAST: Returns a list of existing Python objects (No copy)
# To get positions for a specific symbol:
positions: List[Position] = self.get_positions("NIFTY 50")

# To get ALL positions for the strategy:
all_positions: List[Position] = self.get_positions()

for pos in positions:
    if pos.metadata.get('type') == 'PE':
        # ... logic ...
```

**[+] Performance Note:** `get_active_positions_df()` (accessed via `self.get_positions_df()`) utilizes **Dirty-Flag Caching**. Repeated calls when no positions have changed are **O(1)** (returning a cached reference).

### API Reference

| Method | Returns | Note |
| :--- | :--- | :--- |
| **`get_active_positions(...)`** | **`List[Position]`** (Native) | Primary method for iterative logic. |
| **`get_active_positions_df(...)`** | `pl.DataFrame` (Converted/Cached)| Primary method for vectorized analysis. |
| **`mark_to_market(...)`** | Updates objects in Dict | Optimized for bulk updates. |

---

## Output

### 1. Console Summary
After the simulation, the engine prints a breakdown:
*   Total execution time & throughput (packets/sec).
*   Win Rate & Total PnL per strategy.
*   Overall Portfolio PnL.

### 2. Trade Log (`trades.csv`)
A complete audit trail of every closed trade.

| Key (Entry Time) | ExitTime | Symbol | EntryPrice | ExitPrice | Quantity | Pnl | ExitType |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 2024-01-01 09:15:00 | 2024-01-01 09:20:00 | NIFTY 50 | 21500.0 | 21550.0 | 50 | 2500.0 | STRAT_ID: TAKE_PROFIT |

### 3. Combined Output (`output_{StrategyID}.json`)
A JSON file per strategy integrating both the Closed Trade Log and the Equity Curve (MTM) for visualization purposes.
*   `closedPnl`: List of closed trades (same data as `trades.csv`).
*   `mtm`: Time-series data of Capital, Cumulative PnL, and Drawdown.

---

## Simulation Assumptions

To correctly interpret backtest results, be aware of the following engine behaviors:

1.  **Gross Theoretical PnL**: The system calculates gross profit/loss. It does **not** deduct transaction costs, brokerage commissions, or taxes.
2.  **Instant Execution**: Signals are filled immediately at the timestamp they are generated. There is no simulated latency between signal generation and execution.
3.  **Frictionless Fills**: Orders are filled at the exact 'Close' price (`c`) of the current market data packet (`execution_data`). The engine does not currently model slippage or bid-ask spread impact.
4.  **Sequential Consistency**: While data loading is parallelized, the strategy event loop is strictly sequential to guarantee causal correctness (no lookahead bias).

---

## Reproducibility

To ensure every backtest is fully reproducible, the framework now implements strict **Artifact Snapshotting**.

For every simulation run, the `ArtifactManager` automatically captures:
1.  **Configuration**: The exact `simulation_config.yml` and strategy-specific YAMLs used.
2.  **Code State**: The Git commit hash, branch, and dirty status (saved in `inputs/code_state.json`).
3.  **Strategy Source**: The actual Python source code (`.py` files) of the strategies executed. These are copied to `backtest_results/{Strategy}/{RunID}/inputs/strategies/`.

This guarantees that even if you modify your strategy code later, you always have a pristine copy of the logic that produced a specific set of historical results.
