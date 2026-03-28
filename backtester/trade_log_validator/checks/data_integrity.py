import polars as pl # type: ignore
from typing import List, Dict, Any
from .registry import registry
from ..models.check_result import CheckResult
from ..models.enums import IssueLevel

@registry.register_universal
def check_null_fields(df: pl.DataFrame, context: Dict[str, Any]) -> List[CheckResult]:
    """Detect null/empty values in critical fields."""
    results = []
    # Skip epoch fields as they are derived
    fields_to_check = [c for c in df.columns if c not in ["KeyEpoch", "ExitEpoch", "_row_idx"]]
    
    for field in fields_to_check:
        # Filter for nulls
        null_rows = df.filter(pl.col(field).is_null())
        
        for row in null_rows.iter_rows(named=True):
            results.append(
                CheckResult(
                    row_idx=row["_row_idx"],
                    check_name="No Nulls",
                    issue_type="NULL_VALUE",
                    issue_level=IssueLevel.ERROR,
                    message=f"Field '{field}' is missing/null",
                    metadata={"field": field}
                )
            )
    return results

@registry.register_universal
def check_non_zero(df: pl.DataFrame, context: Dict[str, Any]) -> List[CheckResult]:
    """Ensure critical numeric fields are not zero."""
    results = []
    fields = ['PositionStatus', 'Quantity', 'EntryPrice', 'ExitPrice', 'Pnl']
    
    for field in fields:
        if field not in df.columns:
            continue
            
        zero_rows = df.filter(pl.col(field) == 0)
        
        for row in zero_rows.iter_rows(named=True):
            results.append(
                CheckResult(
                    row_idx=row["_row_idx"],
                    check_name="Non Zero Check",
                    issue_type="ZERO_VALUE",
                    issue_level=IssueLevel.ERROR,
                    message=f"Field '{field}' is zero",
                    metadata={"field": field, "value": 0}
                )
            )
    return results

@registry.register_universal
def check_no_fractional(df: pl.DataFrame, context: Dict[str, Any]) -> List[CheckResult]:
    """Ensure integer fields don't have fractional parts."""
    results = []
    fields = ['PositionStatus', 'Quantity']
    
    for field in fields:
        if field not in df.columns:
            continue
            
        # (val - floor(val)) > 0
        frac_rows = df.filter((pl.col(field) - pl.col(field).floor()) > 0)
        
        for row in frac_rows.iter_rows(named=True):
            results.append(
                CheckResult(
                    row_idx=row["_row_idx"],
                    check_name="Fractional Value Check",
                    issue_type="FRACTIONAL_VALUE",
                    issue_level=IssueLevel.ERROR,
                    message=f"Field '{field}' has fractional part",
                    metadata={"field": field, "value": row[field]}
                )
            )
    return results

@registry.register_universal
def check_no_negatives(df: pl.DataFrame, context: Dict[str, Any]) -> List[CheckResult]:
    """Ensure price and quantity fields are non-negative."""
    results = []
    fields = ['Quantity', 'EntryPrice', 'ExitPrice']
    
    for field in fields:
        if field not in df.columns:
            continue
            
        neg_rows = df.filter(pl.col(field) < 0)
        
        for row in neg_rows.iter_rows(named=True):
            results.append(
                CheckResult(
                    row_idx=row["_row_idx"],
                    check_name="Negative Value Check",
                    issue_type="NEGATIVE_VALUE",
                    issue_level=IssueLevel.ERROR,
                    message=f"Field '{field}' is negative",
                    metadata={"field": field, "value": row[field]}
                )
            )
    return results
