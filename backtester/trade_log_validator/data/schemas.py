import polars as pl # type: ignore

# Expected schema for Trade Logs
TRADE_LOG_SCHEMA = {
    "Key": pl.Utf8,           # Timestamp string for Entry
    "ExitTime": pl.Utf8,      # Timestamp string for Exit
    "Symbol": pl.Utf8,
    "EntryPrice": pl.Float64,
    "ExitPrice": pl.Float64,
    "Quantity": pl.Int64,
    "PositionStatus": pl.Int64, # 1 (Long) or -1 (Short)
    "Pnl": pl.Float64,
    "ExitType": pl.Utf8,      # "Target", "Stoploss", etc.
}

# Expected schema for Lot Sizes
LOT_SIZE_SCHEMA = {
    "Symbol": pl.Utf8,
    "LotSize": pl.Int64,
}
