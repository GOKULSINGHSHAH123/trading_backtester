import polars as pl # type: ignore
from polars import DataFrame # type: ignore
import json
import logging
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
from ..models.check_result import CheckResult

logger = logging.getLogger(__name__)

class ReportGenerator:
    """Generate validation reports and artifacts."""
    
    @staticmethod
    def generate_violations_csv(
        violations: List[CheckResult],
        original_df: DataFrame,
        output_path: Path
    ) -> None:
        """
        Create violations.csv with original row data + issue details.
        """
        final_path = output_path / "violations.csv"
        
        if not violations:
            logger.info("No violations found - skipping violations.csv creation.")
            if final_path.exists():
                final_path.unlink()
            return
        
        # Convert violations to DataFrame
        violation_records = [v.to_dict() for v in violations]
        violations_df = pl.DataFrame(violation_records)
        
        
        output_df = original_df.join(
            violations_df,
            on="_row_idx",
            how="inner"
        )
        
        orig_cols = [c for c in original_df.columns if c != "_row_idx"]
        added_cols = ["CheckName", "IssueType", "IssueLevel", "Message"]
        
        # Ensure added cols exist (they do from join)
        final_cols = orig_cols + added_cols
        
        output_df = output_df.select(final_cols)

        # Cleanup: Drop internal helper columns if they exist
        cols_to_drop = ["KeyDtAdjusted", "ExitDtAdjusted"]
        output_df = output_df.drop([c for c in cols_to_drop if c in output_df.columns])
        
        # Atomic write
        temp_path = output_path / ".violations.csv.tmp"
        output_df.write_csv(temp_path)
        temp_path.rename(final_path)
        
        logger.debug(f"Violations report: {len(violations)} issues written to {final_path}")

    @staticmethod
    def generate_summary(
        total_rows: int,
        violations: List[CheckResult],
        execution_time: float,
        output_path: Path,
        algo_name: str
    ) -> None:
        """Create machine-readable summary JSON."""
        
        # Calculate stats
        violating_rows = {v.row_idx for v in violations}
        valid_rows = total_rows - len(violating_rows)
        pass_rate = (valid_rows / total_rows * 100) if total_rows > 0 else 100.0
        
        summary = {
            "algo_name": algo_name,
            "timestamp": datetime.now().isoformat(),
            "execution_time_seconds": round(execution_time, 2),
            "statistics": {
                "total_rows": total_rows,
                "valid_rows": valid_rows,
                "violating_rows": len(violating_rows),
                "total_violations": len(violations),
                "pass_rate": round(pass_rate, 2)
            },
            "violations_by_type": {},
            "violations_by_level": {}
        }
        
        # Aggregate
        for v in violations:
            summary["violations_by_type"][v.issue_type] = \
                summary["violations_by_type"].get(v.issue_type, 0) + 1
            
            lvl = v.issue_level.value
            summary["violations_by_level"][lvl] = \
                summary["violations_by_level"].get(lvl, 0) + 1
        
        output_file = output_path / "validation_summary.json"
        with open(output_file, 'w') as f:
            json.dump(summary, f, indent=2)
            
        logger.debug(f"Summary written to {output_file}")
