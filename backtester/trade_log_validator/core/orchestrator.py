import logging
import time
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
import polars as pl # type: ignore
from datetime import datetime

from ..data.loaders import load_trade_log, load_lot_sizes
from ..services.orb_client import OrbService
from ..models.api_models import LTPRequest
from ..checks.registry import registry
from ..reporting.generator import ReportGenerator
from ..core.exceptions import ValidationError
from ..core.logger import setup_logging
from ..models.check_result import CheckResult

# Import checks to ensure they register
import backtester.trade_log_validator.checks.data_integrity
import backtester.trade_log_validator.checks.business_rules
import backtester.trade_log_validator.checks.market_data

class ValidationOrchestrator:
    def __init__(self, output_path: str, algo_name: str):
        self.output_path = Path(output_path)
        self.algo_name = algo_name
        self.logger = setup_logging(self.output_path, self.algo_name)
        
    def run(
        self,
        trade_log_path: str,
        lot_size_path: Optional[str],
        segment: str,
        is_commodity_futures: bool,
        orb_config: Dict[str, str]
    ):
        start_time = time.time()
        self.logger.info(f"Starting validation for {self.algo_name} [{segment}]")
        
        try:
            # 1. Load Data
            df = load_trade_log(trade_log_path)
            lot_sizes = load_lot_sizes(lot_size_path)
            
            # 2. Preprocessing & Enrichment
            self.logger.info("Preprocessing timestamps...")
            
            # Parse strings to Datetime
            # We apply a -60s offset here to align Execution Time (Trade Log) with Candle Time (Data)
            # Strategy executes at T using data from T-1 (or T is the close time of the candle starting at T-1)
            df = df.with_columns([
                (pl.col("Key").str.to_datetime(strict=False).dt.replace_time_zone("Asia/Kolkata") - pl.duration(seconds=60)).alias("KeyDtAdjusted"),
                (pl.col("ExitTime").str.to_datetime(strict=False).dt.replace_time_zone("Asia/Kolkata") - pl.duration(seconds=60)).alias("ExitDtAdjusted")
            ])
            
            # Create Epoch columns (Seconds) for checks
            df = df.with_columns([
                (pl.col("KeyDtAdjusted").cast(pl.Int64) / 1_000_000).cast(pl.Int64).alias("KeyEpoch"),
                (pl.col("ExitDtAdjusted").cast(pl.Int64) / 1_000_000).cast(pl.Int64).alias("ExitEpoch")
            ])
            
            # 3. External Data Fetching
            self.logger.info("Preparing market data requests...")
            
            reqs = []
            
            # Entry Requests
            entry_reqs = df.select(["Symbol", "KeyDtAdjusted"]).unique().drop_nulls()
            for row in entry_reqs.iter_rows(named=True):
                reqs.append(LTPRequest(symbol=row["Symbol"], timestamp=row["KeyDtAdjusted"]))
                
            # Exit Requests
            exit_reqs = df.select(["Symbol", "ExitDtAdjusted"]).unique().drop_nulls()
            for row in exit_reqs.iter_rows(named=True):
                reqs.append(LTPRequest(symbol=row["Symbol"], timestamp=row["ExitDtAdjusted"]))
                
            self.logger.info(f"Generated {len(reqs)} unique LTP requests.")
            
            # Init Service
            service = OrbService(
                base_url=orb_config["url"],
                username=orb_config["username"],
                password=orb_config["password"]
            )
            
            # Fetch
            ltp_responses = service.fetch_ltp_data(reqs)
            
            # Build Lookup Map: (Symbol, TimestampSeconds) -> Price
            ltp_map = {}
            for resp in ltp_responses:
                if resp.status == "SUCCESS":
                    key = (resp.symbol, int(resp.timestamp.timestamp()))
                    ltp_map[key] = resp.ltp
            
            self.logger.info(f"Successfully cached {len(ltp_map)} LTP points.")
            
            # 4. Run Checks
            self.logger.info("Executing validation checks...")
            
            context = {
                "ltp_map": ltp_map,
                "lot_sizes": lot_sizes,
                "is_commodity_futures": is_commodity_futures
            }
            
            all_violations = []
            checks = registry.get_checks(segment)
            
            for check_fn in checks:
                check_name = check_fn.__name__
                self.logger.debug(f"Running check: {check_name}")
                try:
                    results = check_fn(df, context)
                    if results:
                        self.logger.debug(f"  -> Found {len(results)} issues.")
                        all_violations.extend(results)
                except Exception as e:
                    self.logger.error(f"Check {check_name} crashed: {e}", exc_info=True)
            
            # 5. Reporting
            execution_time = time.time() - start_time
            
            ReportGenerator.generate_violations_csv(
                all_violations,
                df,
                self.output_path
            )
            
            ReportGenerator.generate_summary(
                total_rows=len(df),
                violations=all_violations,
                execution_time=execution_time,
                output_path=self.output_path,
                algo_name=self.algo_name
            )
            
            self.logger.info("Validation complete.")
            
            # Print Clean Summary to Terminal
            self._print_terminal_summary(len(df), all_violations, execution_time)
            
        except Exception as e:
            self.logger.critical(f"Validation pipeline failed: {e}", exc_info=True)
            self._print_terminal_error(e)
            raise

    def _print_terminal_summary(self, total_rows: int, violations: List[CheckResult], execution_time: float):
        """Prints a well-formatted summary table to stdout."""
        
        violating_rows_count = len({v.row_idx for v in violations})
        valid_rows = total_rows - violating_rows_count
        pass_rate = (valid_rows / total_rows * 100) if total_rows > 0 else 100.0
        
        # Calculate Issue counts
        issue_counts = {}
        for v in violations:
            issue_counts[v.issue_type] = issue_counts.get(v.issue_type, 0) + 1
            
        # Sort issues by count
        sorted_issues = sorted(issue_counts.items(), key=lambda x: x[1], reverse=True)
        
        print("\n" + "="*60)
        print(f" TRADE LOG VALIDATOR REPORT: {self.algo_name}")
        print("="*60)
        print(f" Status:        {'SUCCESS' if violating_rows_count == 0 else 'COMPLETED WITH VIOLATIONS'}")
        print(f" Execution:     {execution_time:.2f}s")
        print(f" Total Trades:  {total_rows}")
        print(f" Passed:        {valid_rows} ({pass_rate:.1f}%)")
        print(f" Failed:        {violating_rows_count}")
        print(f" Total Issues:  {len(violations)}")
        print("-" * 60)
        
        if sorted_issues:
            print(f" {'ISSUE TYPE':<30} | {'COUNT':<10}")
            print("-" * 60)
            for issue, count in sorted_issues:
                print(f" {issue:<30} | {count:<10}")
        else:
            print(" No violations found. Clean log.")
            
        print("="*60)
        print(f" Detailed Report: {self.output_path / 'violations.csv'}")
        print(f" Full Log:        {self.output_path / 'validation.log'}")
        print("="*60 + "\n")

    def _print_terminal_error(self, error: Exception):
        """Prints a clear error message to stdout/stderr."""
        print("\n" + "!"*60)
        print(" VALIDATION FAILED FATALLY")
        print("!"*60)
        print(f" Error: {type(error).__name__}")
        print(f" Details: {str(error)}")
        print("-" * 60)
        print(f" See {self.output_path / 'validation.log'} for full stack trace.")
        print("!"*60 + "\n")