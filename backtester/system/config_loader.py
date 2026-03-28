import yaml
import importlib
import os
import sys
import logging
from dotenv import load_dotenv # type: ignore
from typing import Any, Dict, List, Tuple
from backtester.system.data_provider import BaseDataProvider

# Load environment variables
load_dotenv()

def _load_yaml(path: str) -> Dict[str, Any]:
    """Loads a YAML file from the given path."""
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def _import_class(class_path: str) -> Any:
    """Dynamically imports a class from a string path."""
    try:
        module_path, class_name = class_path.rsplit('.', 1)
        module = importlib.import_module(module_path)
        return getattr(module, class_name)
    except (ImportError, AttributeError) as e:
        raise ImportError(f"Could not import class from path: {class_path}") from e

def load_simulation_config(master_config_path: str) -> Tuple[Dict[str, Any], List[Any], Any, Any]:
    """
    Loads the entire simulation configuration.
    Since 'simulation_config.yml' is now the Single Source of Truth, we treat it 
    as both the master config and the data config.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Loading master configuration from: {master_config_path}")
    master_config = _load_yaml(master_config_path)
    config_dir = os.path.dirname(os.path.abspath(master_config_path))

    # 1. Prepare Data Configuration (Sourced directly from Master Config)
    # We create a clean dict for the DataProvider to avoid passing unnecessary system keys
    data_config = {
        "db_name": master_config.get('db_name', 'data26'),
        "starting_ti_year": master_config['starting_ti_year'],
        "ending_ti_year": master_config['ending_ti_year'],
        "instruments": master_config['instruments'],
        "timeframes": master_config['timeframes'],
        "is_options_segment" : master_config["is_options_segment"],
        "is_commodity_futures": master_config.get("is_commodity_futures", False),
        "validate_results" : master_config["validate_results"],
        "max_instruments_per_collection": master_config.get("max_instruments_per_collection", 50)
    }
    
    # Inject Database URI from Environment
    data_config['db_uri'] = os.getenv('DEST_DB_URI')
    
    if not data_config['db_uri']:
        logger.warning(f"⚠ Warning: Database URI not found in env var 'DEST_DB_URI'.")

    logger.info(f"   - Database: {data_config['db_name']}")

    # 2. Data Provider Class
    provider_class = BaseDataProvider
    logger.info(f"   - Using data provider class: {provider_class.__name__}")

    # 3. Dynamically import the specified Portfolio class
    portfolio_class_path = master_config.get('portfolio_class', 'backtester.system.portfolio.Portfolio')
    portfolio_class = _import_class(portfolio_class_path)
    logger.info(f"   - Loaded portfolio class: '{portfolio_class_path}'")

    # 4. Resolve strategy_root and add to sys.path so strategy modules can be imported
    # Defaults to the directory containing the master config file
    strategy_root = os.path.abspath(
        os.path.join(config_dir, master_config.get("strategy_root", "."))
    )
    if strategy_root not in sys.path:
        sys.path.insert(0, strategy_root)
        logger.info(f"   - Strategy root added to path: {strategy_root}")

    # 5. Load and instantiate strategies
    strategies = []
    for rel_strat_path in master_config['strategy_configs']:
        strat_config_path = os.path.join(config_dir, rel_strat_path)
        strat_config = _load_yaml(strat_config_path)
        logger.info(f"   - Loading strategy config: {strat_config_path}")

        StrategyClass = _import_class(strat_config['strategy_path'])

        full_strategy_config = {
            "strategy_id": strat_config['strategy_id'],
            **strat_config.get('parameters', {})
        }

        instrument_source = strat_config.get('instrument_source', 'all')
        # Use data_config['instruments'] as the source of truth for 'all'
        instruments_to_use = data_config['instruments'] if instrument_source == 'all' else \
                             [inst for inst in data_config['instruments'] if inst in instrument_source]
        
        timeframes = strat_config.get('timeframes', [])
        full_strategy_config['instrument_timeframes'] = {
            instrument: timeframes for instrument in instruments_to_use
        }

        strategy_instance = StrategyClass(
            strategy_id=full_strategy_config['strategy_id'],
            config=full_strategy_config
        )
        strategies.append(strategy_instance)
        logger.info(f"   - Instantiated strategy: '{full_strategy_config['strategy_id']}'")
    
    return data_config, strategies, provider_class, portfolio_class
