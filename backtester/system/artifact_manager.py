"""
Artifact Manager for Backtesting Framework.
Handles experiment tracking, versioning, logging, and result storage.
"""
import os
import shutil
import json
import logging
import subprocess
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any, Callable
from dotenv import load_dotenv # type: ignore
import polars as pl # type: ignore

class ArtifactManager:
    """
    Manages the lifecycle of a simulation run's artifacts.
    Structure: backtest_results/{Strategy_Name}/{SeqNum}_{UserTag}_{Timestamp}/
    """

    ROOT_DIR = "backtest_results"


    def __init__(self, strategy_name: str, user_tag: str = "run", 
                 config_path: Optional[str] = None, is_options_segment: bool = True,
                 is_commodity_futures: bool = False,
                 enable_trade_aggregation: bool = False):
        """
        Initialize the Artifact Manager.

        Args:
            strategy_name: The primary identifier for grouping runs (e.g., 'GridTrader').
            user_tag: A descriptive tag for this specific run (e.g., 'tight_sl').
            config_path: Path to the main simulation config file (for snapshotting).
            is_options_segment: Whether the validation segment should be OPTIONS (True) or UNIVERSAL (False).
            is_commodity_futures: Whether the simulation is for commodity futures.
            enable_trade_aggregation: Whether to aggregate trades in the final CSV.
        """
        self.strategy_name = self._sanitize_name(strategy_name)
        self.user_tag = self._sanitize_name(user_tag)
        self.config_path = config_path
        self.is_options_segment = is_options_segment
        self.is_commodity_futures = is_commodity_futures
        self.enable_trade_aggregation = enable_trade_aggregation

        self.base_path = Path(self.ROOT_DIR) / self.strategy_name
        self.run_id = self._generate_run_id()
        self.run_dir = self.base_path / self.run_id

        self.inputs_dir = self.run_dir / "inputs"
        self.logs_dir = self.run_dir / "logs"
        self.outputs_dir = self.run_dir / "outputs"
        self.validation_dir = self.run_dir / "validation"

        self.logger: Optional[logging.Logger] = None
        self._loggers: Dict[str, logging.Logger] = {}


    def _sanitize_name(self, name: str) -> str:
        """Sanitizes string for filesystem usage."""
        return "".join(c for c in name if c.isalnum() or c in ('-', '_')).strip()


    def _generate_run_id(self) -> str:
        """
        Generates the run directory name: {SeqNum:03d}_{UserTag}_{Timestamp}
        Scans existing directories to determine the next sequence number for the specific UserTag.
        """
        # Ensure base path exists to scan it
        if not self.base_path.exists():
            return f"001_{self.user_tag}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # Scan for existing runs starting with digits
        existing_runs = [
            d.name for d in self.base_path.iterdir() 
            if d.is_dir() and d.name[0].isdigit()
        ]

        seq_num = 1
        if existing_runs:
            indices = []
            for name in existing_runs:
                try:
                    parts = name.split('_')
                    # Expecting: Seq (0), ..., Date (-2), Time (-1)
                    if len(parts) >= 4 and parts[0].isdigit():
                        # Extract tag: everything between Seq and Date
                        extracted_tag = "_".join(parts[1:-2])
                        if extracted_tag == self.user_tag:
                            indices.append(int(parts[0]))
                except (ValueError, IndexError):
                    continue
            
            if indices:
                seq_num = max(indices) + 1

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{seq_num:03d}_{self.user_tag}_{timestamp}"


    def initialize(self):
        """Creates the directory structure on disk."""
        try:
            self.inputs_dir.mkdir(parents=True, exist_ok=True)
            self.logs_dir.mkdir(parents=True, exist_ok=True)
            self.outputs_dir.mkdir(parents=True, exist_ok=True)
            self.validation_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise RuntimeError(
                f"Failed to initialize artifact directories at {self.run_dir}: {e}"
            )


    def setup_logging(self) -> logging.Logger:
        """
        Configures the root logger to write to both file and console.
        Returns the configured logger instance.
        """
        self.initialize()  # Ensure dirs exist

        log_file = self.logs_dir / "execution.log"

        # Create a custom logger
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        # Clear existing handlers to prevent duplicates
        if logger.hasHandlers():
            logger.handlers.clear()

        # 1. File Handler (Detailed with timestamps)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

        # 2. Console Handler (Clean output for user)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(message)s')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

        self.logger = logger
        logger.info(f"[.] Initialized Run Artifact: {self.run_dir}")
        return logger


    def get_logger(self, name: str) -> logging.Logger:
        """
        Creates/retrieves a dedicated logger for a specific name (strategy or symbol).
        Writes to logs/{name}.log and DOES NOT propagate to root.
        """
        if name in self._loggers:
            return self._loggers[name]

        log_file = self.logs_dir / f"{name}.log"
        
        logger = logging.getLogger(f"artifact_{name}")
        logger.setLevel(logging.INFO)
        logger.propagate = False

        if logger.hasHandlers():
            logger.handlers.clear()

        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)

        self._loggers[name] = logger
        return logger


    def get_strategy_logger(self) -> logging.Logger:
        """
        LEGACY: Returns a logger for 'strategy.log'.
        Deprecated in favor of get_logger(strategy_id).
        """
        return self.get_logger("strategy")


    def snapshot_inputs(self, strategy_config_paths: Optional[List[str]] = None):
        """
        Copies configuration files, snapshots Git state, and saves strategy source code
        to the inputs directory.

        Args:
            strategy_config_paths: List of paths to strategy config files to snapshot.
        """
        # 1. Save Main Config
        if self.config_path and os.path.exists(self.config_path):
            shutil.copy2(self.config_path, self.inputs_dir / "simulation_config.yml")

        # 2. Save Strategy Configs & Source Code
        if strategy_config_paths:
            import yaml
            import importlib.util

            # Create dedicated folder for strategy source code
            strategies_src_dir = self.inputs_dir / "strategies"
            strategies_src_dir.mkdir(exist_ok=True)

            for path in strategy_config_paths:
                if os.path.exists(path):
                    # Copy Config
                    dest_name = os.path.basename(path)
                    shutil.copy2(path, self.inputs_dir / dest_name)

                    # Snapshot Source Code
                    try:
                        with open(path, 'r') as f:
                            conf = yaml.safe_load(f)
                        
                        strat_class_path = conf.get('strategy_path')
                        if strat_class_path:
                            # Convert 'strategies.my_strat.MyClass' -> 'strategies.my_strat'
                            module_name = strat_class_path.rsplit('.', 1)[0]
                            spec = importlib.util.find_spec(module_name)
                            
                            if spec and spec.origin:
                                src_file = Path(spec.origin)
                                if src_file.exists():
                                    shutil.copy2(src_file, strategies_src_dir / src_file.name)
                    except Exception as e:
                        if self.logger:
                            self.logger.warning(f"Could not snapshot strategy source for {path}: {e}")

        # 3. Snapshot Git State
        git_info = {
            "commit_hash": "unknown",
            "branch": "unknown",
            "is_dirty": False,
            "timestamp": datetime.now().isoformat()
        }

        try:
            # Check if inside a git repo
            subprocess.check_call(
                ["git", "rev-parse", "--is-inside-work-tree"], 
                stderr=subprocess.DEVNULL, 
                stdout=subprocess.DEVNULL
            )

            git_info["commit_hash"] = subprocess.check_output(
                ["git", "rev-parse", "HEAD"], 
                stderr=subprocess.DEVNULL
            ).strip().decode('utf-8')

            git_info["branch"] = subprocess.check_output(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"], 
                stderr=subprocess.DEVNULL
            ).strip().decode('utf-8')

            status = subprocess.check_output(
                ["git", "status", "--porcelain"], 
                stderr=subprocess.DEVNULL
            ).strip().decode('utf-8')
            if status:
                git_info["is_dirty"] = True

        except (subprocess.CalledProcessError, FileNotFoundError):
            if self.logger:
                self.logger.warning(
                    "Git info could not be retrieved (not a git repo or git not found)."
                )

        with open(self.inputs_dir / "code_state.json", "w") as f:
            json.dump(git_info, f, indent=4)


    def save_results(self, validate: bool = True):
        """
        Moves output files (trades.csv, output_*.json) 
        from root to artifacts/outputs.
        Optionally runs trade log validation.
        """
        # 1. Move master trade log
        trade_log_path = self.outputs_dir / "trades.csv"
        if os.path.exists("trades.csv"):
            try:
                shutil.move("trades.csv", trade_log_path)
                
                # Apply trade aggregation if requested
                if self.enable_trade_aggregation:
                    self._aggregate_trades(trade_log_path)
                    
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Failed to archive/aggregate trades.csv: {e}")
        else:
            if self.logger:
                self.logger.warning("Artifact not found: trades.csv")

        # 2. Move all strategy-specific output files
        import glob
        output_files = glob.glob("output_*.json")
        if output_files:
            for f in output_files:
                try:
                    shutil.move(f, self.outputs_dir / f)
                except Exception as e:
                    if self.logger:
                        self.logger.error(f"Failed to archive {f}: {e}")
        else:
            # Fallback check for old style (just in case)
            if os.path.exists("output.json"):
                try:
                    shutil.move("output.json", self.outputs_dir / "output.json")
                except Exception as e:
                    if self.logger: self.logger.error(f"Failed to archive output.json: {e}")

        if validate:
            self._run_validation()

        if self.logger:
            self.logger.info(
                f"[---] Run Completed. Artifacts saved to: {self.run_dir}"
            )

    def _aggregate_trades(self, file_path: Path):
        """
        Aggregates trades in the given CSV file based on:
        Symbol, PositionStatus, Key (EntryTime), ExitTime, and ExitType (Strategy+Reason).
        Calculates Weighted Average Price (WAP) for Entry and Exit.
        """
        if not file_path.exists():
            return

        try:
            df = pl.read_csv(file_path)
            if df.is_empty():
                return

            if self.logger:
                self.logger.info(f"[*] Aggregating trades in {file_path.name}...")

            # Grouping columns: Symbol, Side, EntryTime (Key), ExitTime, ExitType
            group_cols = ["Symbol", "PositionStatus", "Key", "ExitTime", "ExitType"]
            
            # Ensure all required columns exist
            required_cols = ["Symbol", "PositionStatus", "Key", "ExitTime", "ExitType", "Quantity", "EntryPrice", "ExitPrice", "Pnl"]
            for col in required_cols:
                if col not in df.columns:
                    if self.logger:
                        self.logger.warning(f"[!] Skipping aggregation: Missing column {col}")
                    return

            # Perform aggregation with Weighted Average Price calculation
            agg_df = (
                df.group_by(group_cols)
                .agg([
                    pl.col("Quantity").sum().alias("TotalQuantity"),
                    pl.col("Pnl").sum().alias("TotalPnl"),
                    
                    # Weighted Average Entry Price: Sum(Price * Qty) / Sum(Qty)
                    ((pl.col("EntryPrice") * pl.col("Quantity")).sum() / pl.col("Quantity").sum()).alias("AvgEntryPrice"),
                    
                    # Weighted Average Exit Price: Sum(Price * Qty) / Sum(Qty)
                    ((pl.col("ExitPrice") * pl.col("Quantity")).sum() / pl.col("Quantity").sum()).alias("AvgExitPrice"),
                ])
                .select([
                    pl.col("Key"), 
                    pl.col("ExitTime"), 
                    pl.col("Symbol"), 
                    pl.col("AvgEntryPrice").alias("EntryPrice"), 
                    pl.col("AvgExitPrice").alias("ExitPrice"),
                    pl.col("TotalQuantity").alias("Quantity"), 
                    pl.col("PositionStatus"), 
                    pl.col("TotalPnl").alias("Pnl"), 
                    pl.col("ExitType")
                ])
                .sort("Key")
            )

            agg_df.write_csv(file_path)
            
            if self.logger:
                self.logger.info(f"    - Reduced {len(df)} rows to {len(agg_df)} rows.")

        except Exception as e:
            if self.logger:
                self.logger.error(f"[X] Failed to aggregate trades: {e}")


    def _run_validation(self):
        """
        Internal method to run trade log validation.
        """
        try:
            load_dotenv()

            orb_url = os.getenv('ORB_API_URL')
            orb_username = os.getenv('ORB_CLIENT_ID')
            orb_password = os.getenv('ORB_CLIENT_SECRET')
            
            if not all([orb_url, orb_username, orb_password]):
                if self.logger:
                    self.logger.warning("[!] Skipping validation: Missing ORB credentials in .env")
                return

            # 2. Locate Paths
            lot_size_path = Path(__file__).resolve().parent / "common" / "lot_size.csv"
            trade_log_path = self.outputs_dir / "trades.csv"

            if not trade_log_path.exists():
                 if self.logger: self.logger.warning("[!] Skipping validation: trades.csv not found")
                 return

            # Check if file is empty or only contains header
            try:
                with open(trade_log_path, 'r') as f:
                    lines = f.readlines()
                    if len(lines) <= 1:
                        if self.logger:
                            self.logger.info("[!] Skipping validation: No trade data found in trades.csv (header only).")
                        return
            except Exception as e:
                if self.logger:
                    self.logger.error(f"[X] Failed to read trade log for validation check: {e}")
                return
            
            if not lot_size_path.exists():
                if self.is_commodity_futures:
                    if self.logger: self.logger.warning(f"[!] lot_size.csv not found at {lot_size_path}. Proceeding since is_commodity_futures is True.")
                    lot_size_path = None # type: ignore
                else:
                    if self.logger: self.logger.warning(f"[!] Skipping validation: lot_size.csv not found at {lot_size_path}")
                    return

            # 3. Run Validator
            if self.logger:
                self.logger.info("[*] Running Trade Log Validation...")

            from backtester.trade_log_validator.functional_main import main as validate_trade_log # type: ignore

            segment_str = 'OPTIONS' if self.is_options_segment else 'UNIVERSAL'

            validate_trade_log(
                ORB_URL=orb_url, # type: ignore
                ORB_USERNAME=orb_username, # type: ignore
                ORB_PASSWORD=orb_password, # type: ignore
                algo_name=self.strategy_name,
                lot_size_file_path=str(lot_size_path) if lot_size_path else None, # type: ignore
                trade_log_path=str(trade_log_path),
                segment=segment_str,
                is_commodity_futures=self.is_commodity_futures,
                output_path=str(self.validation_dir)
            )

        except Exception as e:
            if self.logger:
                self.logger.error(f"[X] Validation failed: {e}")
