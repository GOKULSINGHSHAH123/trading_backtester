import polars as pl # type: ignore
from typing import List, Dict, Any
from .registry import registry
from ..models.check_result import CheckResult
from ..models.enums import IssueLevel
from datetime import time

@registry.register_universal
def check_exit_after_entry(df: pl.DataFrame, context: Dict[str, Any]) -> List[CheckResult]:
    """Ensure trade exit occurs after entry."""
    results = []
    
    # If KeyEpoch/ExitEpoch exist, use them (faster)
    if "KeyEpoch" in df.columns and "ExitEpoch" in df.columns:
        invalid_rows = df.filter(pl.col("ExitEpoch") < pl.col("KeyEpoch"))
    else:
        # Fallback to parsing
        invalid_rows = df.filter(
            pl.col("ExitTime").str.to_datetime(strict=False) < pl.col("Key").str.to_datetime(strict=False)
        )
        
    for row in invalid_rows.iter_rows(named=True):
        results.append(
            CheckResult(
                row_idx=row["_row_idx"],
                check_name="Exit < Entry",
                issue_type="INVALID_SEQUENCE",
                issue_level=IssueLevel.ERROR,
                message="Exit timestamp is before Entry timestamp",
                metadata={
                    "entry": row.get("Key"),
                    "exit": row.get("ExitTime")
                }
            )
        )
    return results

@registry.register_universal
def check_market_hours(df: pl.DataFrame, context: Dict[str, Any]) -> List[CheckResult]:
    """Ensure trades occur within market hours."""
    results = []
    
    is_commodity = context.get("is_commodity_futures", False)
    
    if is_commodity:
        start_time = time(9, 0)
        end_time = time(23, 25) # 11:25 PM, allowing buffer for 11:30 PM close
    else:
        start_time = time(9, 15)
        end_time = time(15, 25)
    
    # Parse times
    # We create a temporary DF to avoid modifying the input
    df_times = df.select([
        "_row_idx", "Key", "ExitTime",
        pl.col("Key").str.to_datetime(strict=False).dt.time().alias("EntryTime"),
        pl.col("ExitTime").str.to_datetime(strict=False).dt.time().alias("ExitTimeParsed")
    ])
    
    # Check Entry
    early_entries = df_times.filter(pl.col("EntryTime") < start_time)
    late_entries = df_times.filter(pl.col("EntryTime") > end_time)
    
    # Check Exit
    early_exits = df_times.filter(pl.col("ExitTimeParsed") < start_time)
    late_exits = df_times.filter(pl.col("ExitTimeParsed") > end_time)
    
    # Collect results
    for row in early_entries.iter_rows(named=True):
        results.append(CheckResult(row["_row_idx"], "Market Hours", "OUTSIDE_MARKET_HOURS", IssueLevel.ERROR, f"Entry before {start_time.strftime('%H:%M')}", {"time": str(row["EntryTime"])}))
        
    for row in late_entries.iter_rows(named=True):
        results.append(CheckResult(row["_row_idx"], "Market Hours", "OUTSIDE_MARKET_HOURS", IssueLevel.ERROR, f"Entry after {end_time.strftime('%H:%M')}", {"time": str(row["EntryTime"])}))

    for row in early_exits.iter_rows(named=True):
        results.append(CheckResult(row["_row_idx"], "Market Hours", "OUTSIDE_MARKET_HOURS", IssueLevel.ERROR, f"Exit before {start_time.strftime('%H:%M')}", {"time": str(row["ExitTimeParsed"])}))

    for row in late_exits.iter_rows(named=True):
        results.append(CheckResult(row["_row_idx"], "Market Hours", "OUTSIDE_MARKET_HOURS", IssueLevel.ERROR, f"Exit after {end_time.strftime('%H:%M')}", {"time": str(row["ExitTimeParsed"])}))
        
    return results

@registry.register_universal
def check_pnl_consistency(df: pl.DataFrame, context: Dict[str, Any]) -> List[CheckResult]:
    """Validate PnL calculation."""
    results = []
    
    # Calc Expected PnL
    # ExpectedPnl = Quantity * (ExitPrice - EntryPrice) * PositionStatus
    
    df_calc = df.with_columns(
        (
            pl.col("Quantity") * 
            (pl.col("ExitPrice") - pl.col("EntryPrice")) * 
            pl.col("PositionStatus")
        ).alias("ExpectedPnl")
    )
    
    # Check 1: Mismatch
    mismatch_rows = df_calc.filter(
        (pl.col("ExpectedPnl") - pl.col("Pnl")).abs() > 1e-4
    )
    
    for row in mismatch_rows.iter_rows(named=True):
        results.append(
            CheckResult(
                row_idx=row["_row_idx"],
                check_name="PnL Validation",
                issue_type="PNL_MISMATCH",
                issue_level=IssueLevel.ERROR,
                message=f"Calculated PnL ({row['ExpectedPnl']:.2f}) != Reported PnL ({row['Pnl']:.2f})",
                metadata={"expected": row["ExpectedPnl"], "reported": row["Pnl"]}
            )
        )
        
    # Check 2: Sign Mismatch with ExitType (Warning)
    # ExitType contains "Target" -> PnL should be > 0
    # ExitType contains "Stoploss" -> PnL should be < 0
    
    sign_mismatch = df.filter(
        (pl.col("ExitType").str.contains("Target") & (pl.col("Pnl") < 0)) |
        (pl.col("ExitType").str.contains("Stoploss") & (pl.col("Pnl") > 0))
    )
    
    for row in sign_mismatch.iter_rows(named=True):
        results.append(
            CheckResult(
                row_idx=row["_row_idx"],
                check_name="PnL Reason Validation",
                issue_type="PNL_SIGN_WARNING",
                issue_level=IssueLevel.WARNING,
                message=f"ExitType '{row['ExitType']}' contradicts PnL sign ({row['Pnl']})",
                metadata={"exit_type": row["ExitType"], "pnl": row["Pnl"]}
            )
        )
        
    return results
