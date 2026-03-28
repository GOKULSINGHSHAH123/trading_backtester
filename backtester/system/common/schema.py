from typing import List

# Batching Constants
MAX_INSTRUMENTS_PER_COLLECTION = 50
COLLECTION_BASE_NAME = "ti_data"
daily_candle = "daily_candle"
# Collection Names
BATCH_METADATA_COLLECTION = "batches"
DATA_INSTRUMENT_COLLECTION = "data_instrument"

def get_data_collection_name(batch_num: int) -> str:
    """
    Returns the collection name for a specific data batch.
    Example: batch_num=1 -> "ti_data_1"
    """
    return f"{COLLECTION_BASE_NAME}_{batch_num}"

def get_batch_num_from_index(index: int) -> int:
    """
    Calculates the batch number based on the instrument index.
    0-based index.
    """
    return (index // MAX_INSTRUMENTS_PER_COLLECTION) + 1
