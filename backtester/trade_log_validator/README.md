# Trade Log Validator

The `trade_log_validator` is a self-contained module integrated into the backtester framework to strictly validate simulation outputs (`trades.csv`) against historical market data and business rules.

It is automatically invoked by the `ArtifactManager` at the end of a backtest run.

## Directory Structure

```
trade_log_validator/
├── functional_main.py      # Entry point (Strict Interface)
├── core/
│   ├── orchestrator.py     # Main pipeline coordinator
│   ├── logger.py           # Logging configuration (Console/File)
│   └── exceptions.py       # Custom exception hierarchy
├── data/
│   ├── loaders.py          # CSV ingestion with Polars schema validation
│   └── schemas.py          # Strict type definitions for Trade Logs and Lot Sizes
├── services/
│   └── orb_client.py       # Optimized, batched client for the Orb Market Data API
├── checks/
│   ├── registry.py         # Decorator-based registration system
│   ├── data_integrity.py   # Basic sanity checks (Nulls, Zeros, Negatives)
│   ├── business_rules.py   # Logic checks (Time sequences, PnL consistency, Market Hours)
│   └── market_data.py      # External validation (LTP match, Option Expiry, Lot Sizes)
├── reporting/
│   └── generator.py        # Report generation (Violations CSV & JSON Summary)
└── models/
    ├── check_result.py     # Dataclass for validation results
    ├── api_models.py       # Pydantic models for API requests/responses
    └── enums.py            # Enumerations for Issue Levels and Segments
```

## How It Works

1.  **Ingestion:** The `ValidationOrchestrator` loads the trade log and lot size CSVs using `polars` for high performance. Timestamps are parsed and standardized to UTC-aware Epoch seconds.
2.  **Enrichment:** It scans the trade log to identify every unique `(Symbol, Timestamp)` pair required for validation.
3.  **Data Fetching:** The `OrbService` fetches historical Last Traded Prices (LTP) for these points in optimized batches to minimize API overhead.
4.  **Validation:** Registered checks (`checks/*.py`) run sequentially on the dataframe.
    *   **Universal Checks:** Run on all trades (e.g., PnL math, Time sequence).
    *   **Segment Checks:** Run only for specific segments (e.g., Options Expiry check for `OPTIONS`).
5.  **Reporting:** Violations are aggregated into `violations.csv` and a high-level summary is written to `validation_summary.json`.

## Integration

The validator is called via `system/artifact_manager.py`:

```python
# system/artifact_manager.py

from trade_log_validator.functional_main import main as validate_trade_log

validate_trade_log(
    ORB_URL=orb_url,
    ORB_USERNAME=orb_username,
    ORB_PASSWORD=orb_password,
    algo_name=self.strategy_name,
    lot_size_file_path=str(lot_size_path),
    trade_log_path=str(trade_log_path),
    segment=segment_str,
    output_path=str(self.validation_dir)
)
```

## Adding a New Check

To add a new validation rule, creates a function in `trade_log_validator/checks/` and register it using the decorators from `registry`.

**Example:**

```python
# trade_log_validator/checks/business_rules.py

from .registry import registry
from ..models.check_result import CheckResult, IssueLevel
import polars as pl

@registry.register_universal
def check_max_quantity(df: pl.DataFrame, context: Dict[str, Any]) -> List[CheckResult]:
    """Ensure no trade exceeds 10,000 quantity."""
    results = []
    
    # Filter for violations using Polars (fast)
    bad_rows = df.filter(pl.col("Quantity").abs() > 10000)
    
    # Iterate and create result objects
    for row in bad_rows.iter_rows(named=True):
        results.append(
            CheckResult(
                row_idx=row["_row_idx"],
                check_name="Max Quantity Check",
                issue_type="QTY_TOO_HIGH",
                issue_level=IssueLevel.WARNING,
                message=f"Quantity {row['Quantity']} exceeds limit",
                metadata={"limit": 10000}
            )
        )
    return results
```

The new check will be automatically picked up by the Orchestrator during the next run.
