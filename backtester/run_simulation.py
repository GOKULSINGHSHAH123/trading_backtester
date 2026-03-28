"""
Simulation Runner
=================
Main entry point for the backtesting engine.
Orchestrates configuration loading, artifact management, and simulation execution.

Usage:
    python run_simulation.py --config simulation_config.yml
"""
import argparse
import yaml
import traceback

from backtester.system.config_loader import load_simulation_config
from backtester.system.main import run_simulation
from backtester.system.artifact_manager import ArtifactManager

def main():
    parser = argparse.ArgumentParser(description="Configuration-driven trading backtester.")
    parser.add_argument(
        "--config",
        type=str,
        default="simulation_config.yml",
        help="Path to the master simulation configuration file."
    )
    args = parser.parse_args()

    # Placeholder for manager to ensure cleanup in finally block
    artifact_manager = None
    logger = None

    try:
        # 1. Preliminary Config Load (to get names for Artifact Path)
        with open(args.config, "r") as f:
            raw_config = yaml.safe_load(f)
        
        # Get experiment and run names from config
        experiment_name = raw_config.get("experiment_name", "DefaultExperiment")
        run_tag = raw_config.get("run_tag", "run")
        
        # Get Validation Config
        validate_results = raw_config.get("validate_results", True)
        is_options_segment = raw_config.get("is_options_segment", True)
        is_commodity_futures = raw_config.get("is_commodity_futures", False)
        enable_trade_aggregation = raw_config.get("enable_trade_aggregation", True)
        
        # 2. Initialize Artifact Manager
        artifact_manager = ArtifactManager(
            strategy_name=experiment_name,
            user_tag=run_tag,
            config_path=args.config,
            is_options_segment=is_options_segment,
            is_commodity_futures=is_commodity_futures,
            enable_trade_aggregation=enable_trade_aggregation
        )
        logger = artifact_manager.setup_logging()

        # 3. Load System Objects
        logger.info("[-] Loading Configuration...")
        data_configuration, strategy_instances, provider_class, portfolio_class = load_simulation_config(args.config)
        
        # Snapshot input configs now that we've verified they load
        strategy_config_paths = raw_config.get("strategy_configs", [])
        artifact_manager.snapshot_inputs(strategy_config_paths)

        # 4. Run Simulation
        logger.info(f"[.] Starting Simulation: {len(strategy_instances)} strategies loaded.")

        # --- Setup Strategy Logging (Dynamic / Symbol-scoped) ---
        for strategy in strategy_instances:
            strategy.set_logger_factory(artifact_manager.get_logger)
        logger.info("[.] Strategy logging initialized (isolated per strategy/symbol).")
        
        # Unified Execution Engine (Handles Single and Multi-Strategy)
        run_simulation(data_configuration, strategy_instances, provider_class, portfolio_class)

    except (FileNotFoundError, ImportError, yaml.YAMLError) as e:
        if logger:
            logger.error(f"Configuration Error: {e}")
        else:
            print(f"[ERROR] Configuration Error: {e}")
            
    except Exception as e:
        if logger:
            logger.error(f"Fatal Execution Error: {e}")
            logger.error(traceback.format_exc())
        else:
            print(f"[ERROR] Fatal Error: {e}")
            traceback.print_exc()
            
    finally:
        # 5. Archive Results
        if artifact_manager:
            if logger:
                logger.info("[*] Archiving Simulation Artifacts...")
            artifact_manager.save_results(validate=validate_results)

if __name__ == "__main__":
    main()
