import polars as pl # type: ignore
from pathlib import Path
from typing import Dict, Optional
import logging
from ..core.exceptions import DataIngestionError, ConfigurationError
from .schemas import TRADE_LOG_SCHEMA, LOT_SIZE_SCHEMA

logger = logging.getLogger(__name__)
logger.propagate = False

def load_trade_log(path: str) -> pl.DataFrame:
    """
    Load and validate trade log CSV.
    
    Args:
        path: Path to the trade log CSV file.
        
    Returns:
        Validated Polars DataFrame with an added '_row_idx' column.
        
    Raises:
        DataIngestionError: If file not found or schema mismatch.
    """
    try:
        if not Path(path).exists():
            raise ConfigurationError(f"Trade log file not found: {path}")

        # Load with default string inference to safely check headers first
        # but for now we rely on explicit casting
        df = pl.read_csv(path, infer_schema_length=10000)
        
        # Add row index BEFORE any processing for absolute traceability
        # offset=2 because: 1-based index + 1 for header row = line number in file
        df = df.with_row_index(name="_row_idx", offset=2)
        
        # Schema validation & Type Coercion
        for col, expected_type in TRADE_LOG_SCHEMA.items():
            if col not in df.columns:
                raise DataIngestionError(f"Missing required column in trade log: '{col}'")
            
            try:
                df = df.with_columns(pl.col(col).cast(expected_type, strict=False))
            except Exception as e:
                raise DataIngestionError(f"Column '{col}' cannot be cast to {expected_type}: {e}")
            
        # Strip whitespace from Symbol column for safety
        df = df.with_columns(pl.col("Symbol").str.strip_chars())
        
        logger.info(f"Loaded trade log: {len(df)} rows from {path}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to load trade log: {e}")
        raise DataIngestionError(f"Trade log ingestion failed: {e}") from e


def load_lot_sizes(path: Optional[str]) -> pl.DataFrame:
    """
    Load and validate lot size CSV.
    
    Args:
        path: Path to the lot size CSV file.
        
    Returns:
        Validated Polars DataFrame.
    """
    try:
        if path is None or not Path(path).exists():
            logger.warning(f"Lot size file not found or path is None: {path}. Returning empty DataFrame.")
            return pl.DataFrame(schema=LOT_SIZE_SCHEMA)

        df = pl.read_csv(path)
        
        # Schema validation
        for col, expected_type in LOT_SIZE_SCHEMA.items():
            if col not in df.columns:
                raise DataIngestionError(f"Missing required column in lot size file: '{col}'")
            
            try:
                df = df.with_columns(pl.col(col).cast(expected_type, strict=False))
            except Exception as e:
                raise DataIngestionError(f"Column '{col}' validation failed: {e}")

        # Select ONLY the required columns to drop external junk
        df = df.select(list(LOT_SIZE_SCHEMA.keys()))

        # Trim string columns (Symbol) for safety
        df = df.with_columns(pl.col(pl.Utf8).str.strip_chars())

        # Ensure no duplicates in Symbol
        if df["Symbol"].n_unique() != len(df):
            logger.warning("Duplicate symbols found in lot size file. Keeping first occurrence.")
            df = df.unique(subset=["Symbol"], keep="first")

        logger.info(f"Loaded lot sizes: {len(df)} symbols from {path}")
        return df

    except Exception as e:
        logger.error(f"Failed to load lot sizes: {e}")
        raise DataIngestionError(f"Lot size ingestion failed: {e}") from e
