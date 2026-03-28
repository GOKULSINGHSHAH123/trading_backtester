# Trading Backtester

A high-performance, event-driven backtesting engine for institutional-grade quantitative trading. Built on [Polars](https://pola.rs/) for vectorized data processing and Python `multiprocessing` for parallelized indicator computation — enabling simulation of complex multi-asset strategies over high-frequency data.

The engine eliminates lookahead bias through a strict chronological event loop and supports swappable components via dependency injection.

---

## Key Features

- **Event-Driven Execution** — Simulates live market behavior; strategies react to timestamped packets step-by-step.
- **Hybrid Strategy Architecture** — Supports both **Standard** (`BaseStrategy`) and **Fast** (`BaseStrategyFast`) implementations.
- **Parallel Data Pipeline** — Indicators are pre-calculated in parallel via `get_data_transform` before the simulation loop begins.
- **Strategy Isolation** — Per-strategy portfolio views, isolated logging, and separate state; strategies cannot see each other's trades.
- **Cross-Asset Capable** — Native support for multi-leg options, pair trading, grid trading, and basket strategies.
- **Artifact Snapshotting** — Every run snapshots configs, strategy source code, and Git state for full reproducibility.

---

## Repository Structure

```
trading_backtester/
├── backtester/                        # Core installable package
│   ├── __init__.py                    # Public API: BaseStrategy, Signal, Position, IPortfolio
│   ├── run_simulation.py              # CLI entry point
│   ├── system/
│   │   ├── strategy.py                # BaseStrategy, BaseStrategyFast
│   │   ├── interfaces.py              # IPortfolio protocol, Signal & Position dataclasses
│   │   ├── config_loader.py           # YAML parsing, dynamic strategy import
│   │   ├── main.py                    # Simulation orchestrator loop
│   │   ├── signal_generator.py        # SignalOrchestrator (routing map)
│   │   ├── data_provider.py           # MongoDB → Polars data loading
│   │   ├── execution_engine.py        # Signal → Portfolio trade execution
│   │   ├── portfolio.py               # Default IPortfolio implementation
│   │   ├── artifact_manager.py        # Results archival & logging factory
│   │   └── common/schema.py           # Collection constants
│   ├── trade_log_validator/           # Post-simulation audit & compliance checks
│   └── scripts/run_migration.py       # Data migration CLI
│
├── strategies/                        # User strategies (discovered at runtime)
│   ├── multi_tf_ema_fast/
│   ├── nifty_options/
│   ├── pair_trader/
│   └── rsi_ema_mtf/
│
├── migrator/                          # Migration config
├── pyproject.toml
├── setup.cfg
└── simulation_config.yml              # Master simulation config
```

---

## Quick Start

### 1. Create & Activate Virtual Environment

```bash
# Unix/macOS
python3 -m venv .venv
source .venv/bin/activate

# Windows
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### 2. Install the Package

```bash
pip install -e .
```

### 3. Configure Environment

Create a `.env` file in the project root:

```bash
# MongoDB URI where optimized backtest data is stored
DEST_DB_URI="mongodb://..."

# Orb DAL credentials (for data migration & trade validation)
ORB_API_URL="..."
ORB_CLIENT_ID="username"
ORB_CLIENT_SECRET="api_key"
```

### 4. Migrate Market Data

The backtester runs on a local, optimized MongoDB copy of market data. Run migration before simulating.

Edit `migrator/migration_config.yml` to set date range, instruments, and projections, then:

```bash
backtester-migrate
# or
python backtester/scripts/run_migration.py
```

### 5. Configure the Simulation

Edit `simulation_config.yml`:

```yaml
experiment_name: "my_experiment"
run_tag: "v1"

db_name: "backtest_db"
starting_ti_year: "01-01-2024"
ending_ti_year: "31-03-2024"
buffer_days: 60

instruments:
  - "NIFTY 50"
  - "TCS"

timeframes: ["1m", "5m", "15m"]

strategy_configs:
  - "strategies/multi_tf_ema_fast/multi_tf_ema_fast.yml"
```

### 6. Run the Simulation

```bash
backtester-run
# or
python backtester/run_simulation.py
```

Results are saved to `backtest_results/{experiment_name}/{seq}_{tag}_{timestamp}/`.

---

## System Architecture

```
simulation_config.yml
        │
        ▼
config_loader        →  parse config, import strategies dynamically
        │
        ▼
BaseDataProvider     →  load MongoDB data, apply buffer/warmup window
                     →  run get_data_transform (parallel, per instrument)
                     →  run get_global_transform (sequential, cross-asset)
        │
        ▼  (nested packet per tick)
  {
    "timestamp": ...,
    "1m":  { "current_data": { "NIFTY": {o,h,l,c,v}, ... } },
    "5m":  { "current_data": { ... } },
    "prev_day_candle": { ... }
  }
        │
        ├──► BaseStrategyFast.on_packet(packet, exec_data)  →  List[Signal]
        └──► SignalOrchestrator → BaseStrategy.on_trigger(flat_tick, exec_data)  →  Signal
                                          │
                                          ▼
                              ExecutionEngine  →  portfolio.open_position / close_position
                                          │
                                          ▼
                              ArtifactManager  →  trades.csv, output_{id}.json, logs/
```

---

## Writing a Strategy

### Option A — `BaseStrategyFast` (recommended for options, multi-leg, cross-asset)

Receives the full multi-timeframe market snapshot on every tick.

```python
from backtester import BaseStrategyFast, Signal
import polars as pl

class MyFastStrategy(BaseStrategyFast):

    @staticmethod
    def get_data_transform(df: pl.DataFrame, timeframe: str) -> pl.DataFrame:
        return df.with_columns(pl.col("c").ewm_mean(span=20).alias("EMA_20"))

    def on_packet(self, packet, execution_data):
        tf_5m = packet.get("5m", {}).get("current_data", {})
        nifty = tf_5m.get("NIFTY 50")
        if not nifty:
            return []

        open_positions = self.get_positions("NIFTY 50")
        exec_price = execution_data.get("NIFTY 50", {}).get("c")

        if nifty["c"] > nifty.get("EMA_20", 0) and not open_positions:
            self.log("EMA crossover — entering long", symbol="NIFTY 50")
            return [self.create_signal("NIFTY 50", "BUY", 50, packet["timestamp"], "EMA_CROSS", exec_price)]

        return []
```

### Option B — `BaseStrategy` (recommended for single-instrument, indicator-based logic)

Called once per symbol per timeframe — the engine unrolls the packet for you.

```python
from backtester import BaseStrategy, Signal
import polars as pl

class MyStrategy(BaseStrategy):

    @staticmethod
    def get_data_transform(df: pl.DataFrame, timeframe: str) -> pl.DataFrame:
        return df.with_columns(pl.col("c").ewm_mean(span=20).alias("EMA_20"))

    def on_trigger(self, data, execution_data):
        open_positions = self.get_positions(data["symbol"])

        if data["c"] > data["EMA_20"] and not open_positions:
            return self.create_signal(
                data["symbol"], "BUY", 10, data["timestamp"], "EMA_CROSS", data["c"]
            )
```

### Strategy YAML Config

```yaml
strategy_id: "MY_STRATEGY_001"
strategy_path: "my_strategy_folder.my_strategy.MyStrategy"
instrument_source: all          # or list: ["NIFTY 50", "TCS"]
timeframes: ["1m", "5m"]
parameters:
  quantity: 10
  stop_loss: 0.02
```

---

## Signal API (`create_signal`)

```python
self.create_signal(
    symbol="NIFTY 50",
    action="BUY",           # "BUY", "SELL", or "EXIT"
    quantity=50,
    timestamp=data["timestamp"],
    reason="EMA_CROSS",     # Appears as ExitType in trades.csv
    price=data["c"],
    custom_metadata={},     # Optional
    trade_id_to_close=None  # Required when action="EXIT"
)
```

---

## Position Object

| Property | Type | Description |
|---|---|---|
| `direction` | `str` | `"LONG"` or `"SHORT"` |
| `quantity` | `int` | Signed quantity (positive = long) |
| `entry_price` | `float` | Fill price |
| `trade_id` | `str` | Unique trade identifier |
| `metadata` | `dict` | Custom data from `create_signal` |
| `last_known_price` | `float` | Most recent MTM price |

---

## Logging

```python
self.log("Portfolio rebalanced")                          # → {strategy_id}.log
self.log("Entry at 21500", symbol="NIFTY 50")            # → NIFTY50_{strategy_id}.log
self.log("RSI divergence", symbol="TCS", level="DEBUG")  # DEBUG level
```

Log files are saved under `backtest_results/.../logs/`.

---

## Global Transforms (Cross-Asset Indicators)

Use `get_global_transform` when indicators require multiple instruments (e.g., pair ratios, spreads).

```python
@staticmethod
def get_global_transform(data_map):
    df_tcs  = data_map["TCS"]["1m"]
    df_infy = data_map["INFY"]["1m"]
    combined = df_tcs.join(df_infy, on="ti", suffix="_infy")
    combined = combined.with_columns(
        (pl.col("c") / pl.col("c_infy")).alias("ratio_TCS_INFY")
    )
    data_map["TCS"]["1m"] = combined
    return data_map
```

---

## Output Artifacts

Each run produces a timestamped directory: `backtest_results/{experiment_name}/{seq}_{tag}_{timestamp}/`

```
├── inputs/
│   ├── simulation_config.yml      # Exact config used
│   ├── strategies/                # Snapshot of strategy source code
│   └── code_state.json            # Git commit hash, branch, dirty status
├── outputs/
│   ├── trades.csv                 # Full closed trade log
│   └── output_{strategy_id}.json  # MTM equity curve + closed trades
├── logs/
│   ├── {strategy_id}.log
│   └── {symbol}_{strategy_id}.log
└── validation/
    ├── validation.log
    └── violations.csv
```

### `trades.csv` Schema

| EntryTime | ExitTime | Symbol | EntryPrice | ExitPrice | Quantity | Pnl | ExitType |
|---|---|---|---|---|---|---|---|
| 2024-01-01 09:15 | 2024-01-01 09:20 | NIFTY 50 | 21500.0 | 21550.0 | 50 | 2500.0 | TAKE_PROFIT |

---

## Simulation Assumptions

1. **Gross PnL** — No brokerage, STT, or transaction costs deducted.
2. **Instant Execution** — Orders fill at the tick they are generated (no latency simulation).
3. **No Slippage** — Fills at exact `close` price from `execution_data`.
4. **Sequential Event Loop** — Strategy execution is strictly sequential; no lookahead bias.

---

## Strategy Isolation

Strategies are isolated at four levels:

| Layer | Mechanism |
|---|---|
| **Portfolio** | Nested `open_positions[strategy_id][symbol][trade_id]` — strategies cannot see each other's trades |
| **Data Routing** | `routing_map[(symbol, timeframe)]` — strategies only receive subscribed ticks |
| **Logging** | Logger factory injection — separate log files per strategy/symbol |
| **State** | Each strategy instance has its own `self.state = defaultdict(dict)` |

> All strategies run in the same Python process. This is logical isolation, not OS-level sandboxing. Strategy code is trusted.
