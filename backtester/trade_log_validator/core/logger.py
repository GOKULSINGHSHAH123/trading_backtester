import logging
import sys
from pathlib import Path
from typing import Optional

def setup_logging(output_path: Path, algo_name: str) -> logging.Logger:
    """
    Creates dual-handler logger:
    - File: DEBUG level with full context
    - Console: ERROR level for critical alerts only
    
    Args:
        output_path: Directory where the log file will be created.
        algo_name: Name of the algorithm, used in the logger name.
        
    Returns:
        Configured logger instance.
    """
    logger = logging.getLogger(f"trade_validator.{algo_name}")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    
    # clear existing handlers to prevent duplicates if called multiple times
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Ensure output directory exists
    output_path.mkdir(parents=True, exist_ok=True)
    
    # File handler with detailed formatting
    log_file = output_path / "validation.log"
    file_handler = logging.FileHandler(
        log_file,
        mode='w',
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        '%(asctime)s | %(name)s | %(levelname)-8s | %(funcName)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    
    # Console handler for user feedback
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.ERROR)
    console_formatter = logging.Formatter(
        '%(levelname)s: %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger
