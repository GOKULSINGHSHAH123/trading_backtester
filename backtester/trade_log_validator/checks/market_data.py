import polars as pl # type: ignore
from typing import List, Dict, Any, Tuple
from datetime import datetime
from .registry import registry
from ..models.check_result import CheckResult
from ..models.enums import IssueLevel
from ..utils.orb_utils import OrbUtils

@registry.register_universal
def check_ltp_consistency(df: pl.DataFrame, context: Dict[str, Any]) -> List[CheckResult]:
    """
    Validate Entry/Exit prices against external LTP data.
    Context must contain 'ltp_map': Dict[(symbol, timestamp_int), float]
    """
    results = []
    ltp_map = context.get("ltp_map", {})
    
    if not ltp_map:
        return []
    
    # Let's iterate. Polars iter_rows is decent.
    for row in df.iter_rows(named=True):
        idx = row["_row_idx"]
        sym = row["Symbol"]
        
        # Check Entry
        if row.get("KeyEpoch") is not None:
            entry_ts = row["KeyEpoch"]
            entry_key = (sym, entry_ts)
            
            if entry_key in ltp_map:
                ref_price = ltp_map[entry_key]
                trade_price = row["EntryPrice"]
                if ref_price != trade_price:
                    results.append(CheckResult(idx, "LTP Validation", "LTP_MISMATCH", IssueLevel.ERROR, f"Entry Price {trade_price} != LTP {ref_price}", {"type": "Entry", "ltp": ref_price}))
            else:
                 # Should have been fetched. If missing, it means API returned NOT_FOUND or ERROR.
                 results.append(CheckResult(idx, "LTP Validation", "LTP_MISSING", IssueLevel.WARNING, "LTP data missing for Entry", {"ts": entry_ts}))

        # Check Exit
        if row.get("ExitEpoch") is not None:
            exit_ts = row["ExitEpoch"]
            exit_key = (sym, exit_ts)
            
            if exit_key in ltp_map:
                ref_price = ltp_map[exit_key]
                trade_price = row["ExitPrice"]
                if ref_price != trade_price:
                    results.append(CheckResult(idx, "LTP Validation", "LTP_MISMATCH", IssueLevel.ERROR, f"Exit Price {trade_price} != LTP {ref_price}", {"type": "Exit", "ltp": ref_price}))
            else:
                 results.append(CheckResult(idx, "LTP Validation", "LTP_MISSING", IssueLevel.WARNING, "LTP data missing for Exit", {"ts": exit_ts}))
                 
    return results

@registry.register_options
def check_options_expiry(df: pl.DataFrame, context: Dict[str, Any]) -> List[CheckResult]:
    """Ensure Exit is before Expiry."""
    results = []
    pattern = r"(\d{1,2}[A-Z]{3}\d{2})"
    
    df_expiry = df.select([
        "_row_idx", "Symbol", "ExitEpoch",
        pl.col("Symbol").str.extract(pattern, 1).alias("ExpiryStr")
    ])
    
    # Drop rows where ExpiryStr is null (not an option symbol?)
    df_expiry = df_expiry.filter(pl.col("ExpiryStr").is_not_null())
    
    
    df_expiry = df_expiry.with_columns(
        pl.col("ExpiryStr").str.to_date("%d%b%y", strict=False).add(pl.duration(days=1)).alias("ExpiryDate"),
        pl.col("ExitEpoch").cast(pl.Int64).cast(pl.Datetime("us")).cast(pl.Date).alias("ExitDate") 
        # Comparing dates is safer than timestamps for expiry check
    )
    
    violations = df_expiry.filter(pl.col("ExitDate") > pl.col("ExpiryDate"))
    
    for row in violations.iter_rows(named=True):
        results.append(
            CheckResult(
                row_idx=row["_row_idx"],
                check_name="Options Expiry",
                issue_type="EXPIRED_TRADE",
                issue_level=IssueLevel.ERROR,
                message=f"Exit Date {row['ExitDate']} is after Expiry {row['ExpiryDate']}",
                metadata={"expiry": str(row['ExpiryDate'])}
            )
        )
        
    return results

@registry.register_options
def check_lot_size(df: pl.DataFrame, context: Dict[str, Any]) -> List[CheckResult]:
    """Validate Quantity matches Lot Size."""
    # Skip lot size checks for commodity futures
    if context.get("is_commodity_futures", False):
        return []

    results = []
    lot_sizes = context.get("lot_sizes") # pl.DataFrame
    
    if lot_sizes is None:
        return []
        
    pattern = r"^([A-Z0-9.&_\-]+?)\d{1,2}[A-Z]{3}\d{2}"
    
    df_sym = df.with_columns(
        pl.col("Symbol").str.extract(pattern, 1).alias("Underlying")
    )
    
    
    joined = df_sym.join(
        lot_sizes, 
        left_on="Underlying", 
        right_on="Symbol", 
        how="left"
    )
    
    # Check 1: Unknown Symbol (LotSize is null)
    unknowns = joined.filter(pl.col("LotSize").is_null())
    for row in unknowns.iter_rows(named=True):
         if row["Underlying"] is not None: # Only if regex matched
             results.append(
                 CheckResult(
                     row["_row_idx"], 
                     "Lot Size", 
                     "UNKNOWN_SYMBOL", 
                     IssueLevel.ERROR, 
                     f"No lot size found for '{row['Underlying']}'", 
                     {"extracted": row["Underlying"]}
                 )
             )

    # Check 2: Quantity Mismatch
    mismatches = joined.filter(
        (pl.col("LotSize").is_not_null()) & 
        (pl.col("Quantity") % pl.col("LotSize") != 0)
    )
    
    for row in mismatches.iter_rows(named=True):
        results.append(
            CheckResult(
                row_idx=row["_row_idx"], 
                check_name="Lot Size", 
                issue_type="QTY_MISMATCH", 
                issue_level=IssueLevel.ERROR, 
                message=f"Quantity {row['Quantity']} != Lot Size {row['LotSize']}", 
                metadata={"expected": row["LotSize"]}
            )
        )
        
    return results
