"""
The preloading component that migrates data using the Orb Data Access Layer (DAL) API.
"""

import os
import threading
import polars as pl # type: ignore
import requests # type: ignore
from requests.adapters import HTTPAdapter # type: ignore
from pymongo import MongoClient, UpdateOne # type: ignore
from tqdm import tqdm # type: ignore
import time
import logging
from datetime import datetime, timezone, timedelta
from dateutil.relativedelta import relativedelta
from typing import Dict, List, Tuple, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from backtester.system.common import schema

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class OrbAPIClient:
    """
    A client to interact with the Orb Data Access Layer API.
    Handles authentication and provides methods for data fetching.
    """
    def __init__(self, api_url: str, client_id: str, client_secret: str, pool_size: int = 50):
        self.api_url = api_url.rstrip('/')
        self.auth_data = {"username": client_id, "password": client_secret}
        self.session = requests.Session()
        
        # Configure Connection Pool
        adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        self.authenticate()

    def authenticate(self):
        """Gets a bearer token and sets it in the session headers."""
        logger.info("Authenticating with Orb DAL...")
        try:
            response = self.session.post(f"{self.api_url}/api/auth/token", data=self.auth_data)
            response.raise_for_status()
            token = response.json()["access_token"]
            self.session.headers.update({"Authorization": f"Bearer {token}"})
            logger.info("✓ Orb DAL authentication successful.")
        except requests.exceptions.RequestException as e:
            logger.error(f"Fatal: Failed to authenticate with Orb DAL: {e}")
            if e.response is not None:
                logger.error(f"Response Body: {e.response.text}")
            raise

    def find(self, db: str, collection: str, query: Dict, projection: Optional[Dict] = None, sort: Optional[List] = None, limit: Optional[int] = None, offset: Optional[int] = None) -> List[Dict]:
        """Performs a 'find' operation via the Orb DAL."""
        payload = {
            "db": db,
            "collection": collection,
            "query": query,
            "projection": projection,
            "sort": sort,
            "limit": limit,
            "offset": offset
        }
        # Remove None values so we don't send empty fields that might fail validation
        payload = {k: v for k, v in payload.items() if v is not None}
        
        try:
            response = self.session.post(f"{self.api_url}/api/data/find", json=payload)
            response.raise_for_status()
            if not response.json():
                logger.info(f"No data found for {db}.{collection} with query: {query} | status code: {response.status_code}")
            
            return response.json().get('data')
        except requests.exceptions.RequestException as e:
            logger.error(f"API find call failed for {db}.{collection}: {e}")
            if e.response is not None:
                logger.error(f"Response Body: {e.response.text}")
            raise

    def count(self, db: str, collection: str, query: Dict) -> int:
        """Performs a 'count_documents' operation via the Orb DAL."""
        payload = {"db": db, "collection": collection, "query": query}
        try:
            response = self.session.post(f"{self.api_url}/api/data/count_documents", json=payload)
            response.raise_for_status()
            return response.json()["count"]
        except requests.exceptions.RequestException as e:
            logger.error(f"API count call failed for {db}.{collection}: {e}")
            if e.response is not None:
                logger.error(f"Response Body: {e.response.text}")
            raise


class MongoDBDataMigrator:
    """
    Handles data migration using the Orb DAL for source data access.
    Uses system.common.schema for collection naming and organization.
    """

    # NOTE: Increase PAGE_SIZE to the maximum your Orb DAL API supports.
    # Each increase directly reduces API round trips proportionally.
    # e.g. 200_000 = 4x fewer calls than 50_000. Benchmark from 100_000 upward.
    PAGE_SIZE = 100_000
    MAX_RETRIES = 3
    SECONDS_PER_DAY = 24 * 60 * 60
    SECONDS_PER_HOUR = 3600
    # Increased from 2x CPU — this is I/O bound against a remote API,
    # threads spend most time waiting on network, not on CPU.
    MAX_WORKERS = max(8, (os.cpu_count() or 1) * 4)

    # Symbol mapping for Options instrument name -> Underlying spot symbol
    # Used for: spot enrichment query, options 'u' field query, daily candle derivation
    SYMBOL_MAPPING = {
        "NIFTY": "NIFTY 50",
        "SENSEX": "SENSEX",
        "BANKNIFTY": "NIFTY BANK",
    }

    def __init__(
        self,
        config: Dict[str, Any],
        dest_uri: str,
        orb_api_url: str,
        orb_client_id: str,
        orb_client_secret: str,
        buffer_months: int = 1
    ):
        """
        Initialize the Migrator.
        
        Args:
            buffer_months: Number of months to include as buffer before starting_date
        """
        self.config = config
        self.dest_uri = dest_uri
        self.orb_api_url = orb_api_url
        self.orb_client_id = orb_client_id
        self.orb_client_secret = orb_client_secret
        
        # Unpack Config
        self.dest_db_name = config['db_name']
        self.starting_date = config['starting_date']
        self.ending_date = config['ending_date']
        self.expiry_days = config.get('expiry_days', 14)
        self.buffer_months = buffer_months
        
        # Extract projection dictionaries from config for source queries
        self.underlying_projection = config['underlying_projection']
        self.options_projection = config['options_projection']
        self.futures_projection = config.get('futures_projection', {})
        
        # Extract projection keys (fields to keep) from config for destination documents
        self.underlying_fields = [k for k, v in self.underlying_projection.items() if v and k not in ('_id', 'sym', 'ti')]
        self.options_fields = [k for k, v in self.options_projection.items() if v and k not in ('_id', 'sym', 'ti')]
        self.futures_fields = [k for k, v in self.futures_projection.items() if v and k not in ('_id', 'sym', 'ti')]
        
        logger.info(f"Underlying data fields: {self.underlying_fields}")
        logger.info(f"Options data fields: {self.options_fields}")
        logger.info(f"Futures data fields: {self.futures_fields}")
        
        # Date Processing with Buffer
        self.start_dt = datetime.strptime(self.starting_date, "%d-%m-%Y")
        self.end_dt = datetime.strptime(self.ending_date, "%d-%m-%Y")
        
        # Calculate buffer start date (N months before start date)
        self.buffer_start_dt = self.start_dt - relativedelta(months=self.buffer_months)
        
        # Convert to Unix timestamps
        self.buffer_start_unix = int(self.buffer_start_dt.replace(tzinfo=timezone.utc).timestamp())
        self.start_unix = int(self.start_dt.replace(tzinfo=timezone.utc).timestamp())
        self.end_unix = int(self.end_dt.replace(tzinfo=timezone.utc).timestamp())
        
        # Get years range including buffer
        self.buffer_start_year = self.buffer_start_dt.year
        self.start_year = self.start_dt.year
        self.end_year = self.end_dt.year
        
        # Create list of all years to process (including buffer year if different)
        all_years = set()
        all_years.update(range(self.buffer_start_year, self.end_year + 1))
        self.all_collection_years = sorted(all_years)
        
        logger.info(f"\n{'=' * 60}")
        logger.info("DATE CONFIGURATION:")
        logger.info(f"{'=' * 60}")
        logger.info(f"Buffer Start Date: {self.buffer_start_dt.strftime('%d-%m-%Y')} (Year: {self.buffer_start_year})")
        logger.info(f"Config Start Date: {self.start_dt.strftime('%d-%m-%Y')} (Year: {self.start_year})")
        logger.info(f"Config End Date: {self.end_dt.strftime('%d-%m-%Y')} (Year: {self.end_year})")
        logger.info(f"Years to process: {self.all_collection_years}")
        logger.info(f"{'=' * 60}")
        
        # State
        self.api_client: Optional[OrbAPIClient] = None
        self.dest_client: Optional[MongoClient] = None
        self.dest_db = None
        self.data_instrument_collection = None
        self.batches_collection = None
        self.expiry_loaded = False  # Track if expiry has been loaded

        # Per instrument-year expiry cutoff cache — avoids recomputing on every page.
        # Reset at the start of each _process_options call.
        self._expiry_cutoff_cache: Optional[pl.DataFrame] = None
        
        # Concurrency & Safety
        self.lock = threading.Lock()
        # Separate lock for expiry cache since it's written from multiple threads
        self._expiry_cache_lock = threading.Lock()
        
        # Stats
        self.stats = {
            "processed": 0,
            "uploaded": 0,
            "modified": 0,
            "failed_batches": 0
        }
        
        self.all_dbs = []
        self.global_batches = []

    @staticmethod
    def _date_to_unix(date_str: str) -> int:
        dt = datetime.strptime(date_str, "%d-%m-%Y")
        return int(dt.replace(tzinfo=timezone.utc).timestamp())

    def get_year_query(self, year: int) -> Dict[str, int]:
        """Returns MongoDB time query for a specific year collection."""
        year_start = int(datetime(year, 1, 1, tzinfo=timezone.utc).timestamp())
        year_end = int(datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp())
        
        if year == self.buffer_start_year == self.start_year == self.end_year:
            return {"$gte": self.buffer_start_unix, "$lte": self.end_unix}
        elif year == self.buffer_start_year and self.buffer_start_year != self.start_year:
            return {"$gte": self.buffer_start_unix, "$lte": year_end}
        elif year == self.start_year == self.end_year:
            return {"$gte": self.start_unix, "$lte": self.end_unix}
        elif year == self.start_year:
            return {"$gte": self.start_unix, "$lte": year_end}
        elif year == self.end_year:
            return {"$gte": year_start, "$lte": self.end_unix}
        else:
            return {"$gte": year_start, "$lte": year_end}

    def configure_databases(self):
        """Parse instrument lists from config and create global batches."""
        self.all_dbs = []
        all_instruments_flat = []
        
        logger.info("=" * 60)
        logger.info("CONFIG DEBUG INFO:")
        logger.info("=" * 60)
        
        def collect_instruments(config_key: str, is_option: object):
            instruments_list = self.config.get(config_key)
            if instruments_list:
                db_name = self.config['source_db_names'].get(config_key, f"{config_key}_db")
                self.all_dbs.append({db_name: instruments_list})
                logger.info(f"✓ Added {db_name} with {len(instruments_list)} instruments")
                for inst in instruments_list:
                    all_instruments_flat.append((db_name, inst, is_option))

        collect_instruments('index_options', "options")
        collect_instruments('stock_options', "options")
        collect_instruments('index', "stock") 
        collect_instruments('stocks', "stock")
        collect_instruments('stock_futures', "future")
        collect_instruments('index_futures', "future")
        collect_instruments('commodity_futures', "future")
            
        if not all_instruments_flat:
            raise ValueError("No instruments configured in migration_config.yml")
            
        logger.info(f"Total instruments across all DBs: {len(all_instruments_flat)}")
        logger.info("=" * 60)
        
        logger.info("\n" + "=" * 60)
        logger.info("CREATING GLOBAL BATCHES:")
        logger.info("=" * 60)
        
        self.global_batches = []
        batch_size = schema.MAX_INSTRUMENTS_PER_COLLECTION
        
        for i in range(0, len(all_instruments_flat), batch_size):
            chunk = all_instruments_flat[i : i + batch_size]
            batch_num = schema.get_batch_num_from_index(i)
            
            db_groups = {}
            batch_instruments_only = []
            
            for source_db_name, inst, is_option in chunk:
                if source_db_name not in db_groups:
                    db_groups[source_db_name] = {'instruments': [], 'is_option': is_option}
                db_groups[source_db_name]['instruments'].append(inst)
                batch_instruments_only.append(inst)
            
            self.global_batches.append({
                "batch_num": batch_num,
                "db_groups": db_groups,
                "all_instruments": batch_instruments_only
            })
            
            logger.info(f"\nBatch {batch_num}:")
            logger.info(f"  Total instruments: {len(batch_instruments_only)}")
            for db_name, info in db_groups.items():
                logger.info(f"    - {db_name}: {len(info['instruments'])} instruments")
        
        logger.info(f"\n✓ Created {len(self.global_batches)} global batches")
        logger.info("=" * 60 + "\n")

    def connect(self):
        """Connect to Orb DAL and Destination Mongo, then setup collections."""
        logger.info("Initializing Orb DAL API Client...")
        self.api_client = OrbAPIClient(self.orb_api_url, self.orb_client_id, self.orb_client_secret)
        
        logger.info("Connecting to Destination MongoDB...")
        self.dest_client = MongoClient(
            self.dest_uri,
            serverSelectionTimeoutMS=60000,
            socketTimeoutMS=60000,
            connectTimeoutMS=60000,
            maxPoolSize=50,
        )
        self.dest_client.admin.command("ping") # type: ignore
        logger.info("✓ Destination MongoDB connected")
        
        self.dest_db = self.dest_client[self.dest_db_name] # type: ignore
        
        # Setup schema-defined collections
        self.data_instrument_collection = self.dest_db[schema.DATA_INSTRUMENT_COLLECTION]
        self.data_instrument_collection.create_index([("sym", 1)], background=True)
        logger.info("✓ Created/Ensured data_instrument collection with index")
        
        self.batches_collection = self.dest_db[schema.BATCH_METADATA_COLLECTION]
        self.batches_collection.create_index([("batch_name", 1)], background=True)
        logger.info("✓ Created/Ensured batches metadata collection")

    def disconnect(self):
        if self.dest_client: self.dest_client.close()
        logger.info("✓ All connections closed.")

    @staticmethod
    def get_expiry_mapping_df(options_df: pl.DataFrame, expiry_days: int) -> pl.DataFrame:
        """Calculate cutoff timestamps for options."""
        cutoff_seconds = expiry_days * MongoDBDataMigrator.SECONDS_PER_DAY
        
        return (
            options_df
            .with_columns(
                pl.col("sym").str.extract(r"(\d{2}[A-Z]{3}\d{2})", 1).alias("expiry_str")
            )
            .filter(pl.col("expiry_str").is_not_null())
            .with_columns(
                pl.col("expiry_str")
                .str.strptime(pl.Date, "%d%b%y", strict=False)
                .alias("expiry_date_base")
            )
            .with_columns(
                (pl.col("expiry_date_base").cast(pl.Datetime) + pl.duration(hours=15, minutes=30))
                .alias("expiry_date")
            )
            .with_columns(
                (pl.col("expiry_date").cast(pl.Int64) // 1_000_000).alias("expiry_unix")
            )
            .select(["sym", "expiry_unix"])
            .unique()
            .with_columns((pl.col("expiry_unix") - cutoff_seconds).alias("cutoff_unix"))
            .select(["sym", "cutoff_unix"])
        )

    def apply_expiry_filter(self, options_df: pl.DataFrame) -> pl.DataFrame:
        """
        Filter options dataframe based on expiry proximity.
        Uses a cached cutoff map that grows incrementally as new syms are seen,
        avoiding full recomputation on every page.
        """
        if not self.expiry_days:
            return options_df

        with self._expiry_cache_lock:
            if self._expiry_cutoff_cache is None:
                # First page — build cache from scratch
                self._expiry_cutoff_cache = self.get_expiry_mapping_df(options_df, self.expiry_days)
            else:
                # Subsequent pages — only compute for syms not yet cached
                known_syms = self._expiry_cutoff_cache["sym"]
                new_syms_df = options_df.filter(~pl.col("sym").is_in(known_syms))
                if len(new_syms_df) > 0:
                    new_cutoffs = self.get_expiry_mapping_df(new_syms_df, self.expiry_days)
                    if len(new_cutoffs) > 0:
                        self._expiry_cutoff_cache = pl.concat(
                            [self._expiry_cutoff_cache, new_cutoffs]
                        )
            
            cutoff_snapshot = self._expiry_cutoff_cache

        return (
            options_df.lazy()
            .join(cutoff_snapshot.lazy(), on="sym", how="inner") # type: ignore
            .filter(pl.col("ti") >= pl.col("cutoff_unix"))
            .drop("cutoff_unix")
            .collect()
        )

    def _upload_data_batch(self, collection, rows: Dict, max_retries: int = MAX_RETRIES) -> Tuple[int, int, bool]:
        """Bulk write time-interval data to MongoDB."""
        for retry in range(max_retries):
            try:
                operations = []
                for ti_str, symbols_map in rows.items():
                    ti_int = int(ti_str)
                    update_doc = {f"{ti_str}.{sym}": ohlc for sym, ohlc in symbols_map.items()}
                    
                    if not update_doc: continue
                    
                    operations.append(UpdateOne(
                        {"_id": ti_int},
                        {"$set": update_doc},
                        upsert=True
                    ))
                
                if not operations: return 0, 0, True
                
                result = collection.bulk_write(operations, ordered=False)
                return result.upserted_count, result.modified_count, True
                
            except Exception as e:
                logger.warning(f"Batch upload error (Attempt {retry+1}): {e}")
                if retry < max_retries - 1:
                    time.sleep(5)
        
        return 0, 0, False

    def _upload_spot_meta_batch(self, collection, docs: List[Dict], max_retries: int = MAX_RETRIES) -> Tuple[int, int, bool]:
        """Bulk write spot metadata/OHLC to data_instrument collection."""
        for retry in range(max_retries):
            try:
                if not docs: return 0, 0, True
                operations = []
                for d in docs:
                    filter_q = {"_id": d["_id"]} if "_id" in d else {"ti": d.get("ti"), "sym": d.get("sym")}
                    update_doc = {k: v for k, v in d.items() if k != "_id"}
                    
                    operations.append(UpdateOne(filter_q, {"$set": update_doc}, upsert=True))
                
                result = collection.bulk_write(operations, ordered=False)
                return result.upserted_count, result.modified_count, True
                
            except Exception as e:
                logger.warning(f"Spot upload error (Attempt {retry+1}): {e}")
                if retry < max_retries - 1:
                    time.sleep(5)
        
        return 0, 0, False

    def _fetch_and_process_time_range(
        self,
        db_name,
        year,
        base_query,
        projection,
        start_ts,
        end_ts,
        dest_col,
        ti_data_col,
        spot_dict=None,
        is_spot=False,
        fields=None,
    ):
        """
        Worker function to fetch and process data for a specific time range (chunk).
        Uses hourly chunks for maximum parallelism.

        `fields` controls which keys are written into the timeseries document.
        - Spot path:    callers pass self.underlying_fields  (or self.futures_fields for futures)
        - Options path: self.options_fields are used inside _flush_buffer directly
        - When None:    falls back to self.underlying_fields (preserves original behaviour)

        The count pre-check has been removed — it was an extra remote round trip
        per chunk with no benefit since the find handles empty results gracefully.
        """
        effective_fields = fields if fields is not None else self.underlying_fields

        try:
            # Construct the time-bounded query for this chunk
            chunk_query = base_query.copy()
            chunk_query["ti"] = {"$gte": start_ts, "$lt": end_ts}
            
            # Internal pagination loop for the chunk
            offset = 0
            total_chunk_docs = 0
            
            while True:
                try:
                    page_docs = self.api_client.find( # type: ignore
                        db=db_name,
                        collection=str(year),
                        query=chunk_query,
                        projection=projection,
                        sort=[["ti", 1], ["sym", 1]],
                        limit=self.PAGE_SIZE,
                        offset=offset
                    )
                    
                    if not page_docs:
                        break
                    
                    count = len(page_docs)
                    total_chunk_docs += count
                    offset += count
                    
                    rows = {}
                    if is_spot:
                        # 1. Upload Metadata (Spot / Futures)
                        self._upload_spot_meta_batch(dest_col, page_docs)
                        
                        # 2. Process Timeseries using the caller-supplied field list
                        for doc in page_docs:
                            ti = doc.get("ti")
                            if not ti: continue
                            ti_shifted = str(ti + 60)
                            sym = doc.get("sym")
                            if ti_shifted not in rows:
                                rows[ti_shifted] = {}
                            rows[ti_shifted][sym] = {
                                k: doc.get(k) for k in effective_fields if doc.get(k) is not None
                            }
                    else:
                        # Process Options
                        self._flush_buffer(page_docs, rows, spot_dict, ti_data_col)
                    
                    # Upload Batch (options are flushed inside _flush_buffer)
                    if is_spot and rows:
                        u, m, ok = self._upload_data_batch(ti_data_col, rows)
                        with self.lock:
                            if ok:
                                self.stats['uploaded'] += u
                                self.stats['modified'] += m
                            else:
                                self.stats['failed_batches'] += 1
                    
                    if count < self.PAGE_SIZE:
                        break
                        
                except Exception as page_error:
                    logger.warning(f"Error fetching page for {db_name}.{year} (offset={offset}): {page_error}")
                    if "collection" in str(page_error).lower() and "not found" in str(page_error).lower():
                        logger.info(f"Collection {db_name}.{year} not found, skipping...")
                        break
                    else:
                        raise
            
            return total_chunk_docs

        except Exception as e:
            logger.error(f"Error processing time range {start_ts}-{end_ts}: {e}")
            with self.lock:
                self.stats['failed_batches'] += 1
            return 0

    def _generate_daily_ranges(self, year: int) -> List[Tuple[int, int]]:
        """Generates (start, end) unix timestamps for each day in the given year/config range."""
        year_start = int(datetime(year, 1, 1, tzinfo=timezone.utc).timestamp())
        year_end = int(datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp())
        
        if year == self.buffer_start_year and self.buffer_start_year != self.start_year:
            actual_start = self.buffer_start_unix
            actual_end = year_end
        elif year == self.start_year == self.end_year:
            actual_start = self.start_unix
            actual_end = self.end_unix
        elif year == self.start_year:
            actual_start = self.start_unix
            actual_end = year_end
        elif year == self.end_year:
            actual_start = year_start
            actual_end = self.end_unix
        else:
            actual_start = year_start
            actual_end = year_end
        
        ranges = []
        current = actual_start
        while current < actual_end:
            next_day = current + self.SECONDS_PER_DAY
            ranges.append((current, min(next_day, actual_end + 1)))
            current = next_day
        return ranges

    def _generate_hourly_ranges(self, year: int) -> List[Tuple[int, int]]:
        """
        Generates (start, end) unix timestamps at hourly granularity.
        Finer chunks = more parallel workers active simultaneously.
        For 100M+ docs/day, daily chunks result in thousands of sequential
        pages per worker. Hourly chunks spread that load across more workers.
        """
        daily_ranges = self._generate_daily_ranges(year)
        hourly_ranges = []
        for day_start, day_end in daily_ranges:
            current = day_start
            while current < day_end:
                next_hour = current + self.SECONDS_PER_HOUR
                hourly_ranges.append((current, min(next_hour, day_end)))
                current = next_hour
        return hourly_ranges

    def _flush_buffer(self, buffer, rows, spot_dict, ti_data_col):
        df = pl.DataFrame(buffer)
        df = self.apply_expiry_filter(df)
        
        for row in df.iter_rows(named=True):
            ti = row["ti"] + 60
            ti_str = str(ti)
            if ti_str not in rows:
                rows[ti_str] = {}
            
            rows[ti_str][row["sym"]] = {
                k: row.get(k) for k in self.options_fields if row.get(k) is not None
            }
            
            if ti_str in spot_dict:
                for s_sym, s_data in spot_dict[ti_str].items():
                    rows[ti_str][s_sym] = s_data
        
        if len(rows) > 0:
            u, m, ok = self._upload_data_batch(ti_data_col, rows)
            with self.lock:
                if ok:
                    self.stats['uploaded'] += u
                    self.stats['modified'] += m
                else:
                    self.stats['failed_batches'] += 1
            rows.clear()

    def _pre_load_daily_candle(self, source_db_name: str, source_col_name: str, dest_col: str):
        """
        Loads daily candle data from source database via Orb API
        and dumps to destination database under the specified collection.
        Derives instruments from both direct spot lists (index/stocks) and
        options lists (via SYMBOL_MAPPING + spot_db_for_options), so daily
        candle populates correctly even when index/stocks config lists are empty.
        """
        try:
            dest_collection = self.dest_client[self.dest_db_name][dest_col] # type: ignore
            dest_collection.create_index([("sym", 1), ("ti", 1)], background=True)

            instruments_to_load = []  # list of (spot_sym, source_db_name)
            spot_db_for_options = self.config.get('spot_db_for_options', {})

            for db_group in self.all_dbs:
                for db_name, instruments in db_group.items():

                    # Case 1: direct spot DBs (index, stocks)
                    spot_dbs = {self.config['source_db_names'].get(k) for k in ('index', 'stocks')}
                    if db_name in spot_dbs:
                        for inst in instruments:
                            instruments_to_load.append((inst, source_db_name))

                    # Case 2: options DBs — derive underlying via SYMBOL_MAPPING
                    elif db_name in spot_db_for_options:
                        for inst in instruments:
                            spot_sym = self.SYMBOL_MAPPING.get(inst, inst)
                            instruments_to_load.append((spot_sym, source_db_name))

            # Deduplicate — same underlying may appear from multiple option DBs
            instruments_to_load = list(dict.fromkeys(instruments_to_load))

            if not instruments_to_load:
                logger.warning("No instruments found for daily candle preload.")
                return

            logger.info(f"Loading daily candle data for {len(instruments_to_load)} instruments from {source_db_name}.{source_col_name}...")
            total_inserted = 0

            for instrument, src_db in instruments_to_load:
                try:
                    query = {
                        "sym": instrument,
                        "ti": {
                            "$gte": self.buffer_start_unix,
                            "$lte": self.end_unix
                        }
                    }

                    try:
                        count = self.api_client.count( # type: ignore
                            db=source_db_name,
                            collection=source_col_name,
                            query=query
                        )
                        logger.info(f"  • {instrument}: {count} daily candle records found")
                        if count == 0:
                            continue
                    except Exception as count_error:
                        logger.warning(f"  • Could not count daily candle docs for {instrument}: {count_error}")

                    offset = 0
                    instrument_total = 0

                    while True:
                        page_docs = self.api_client.find( # type: ignore
                            db=source_db_name,
                            collection=source_col_name,
                            query=query,
                            sort=[["ti", 1], ["sym", 1]],
                            limit=self.PAGE_SIZE,
                            offset=offset
                        )

                        if not page_docs:
                            break

                        operations = [
                            UpdateOne(
                                {"sym": doc.get("sym"), "ti": doc.get("ti")},
                                {"$set": doc},
                                upsert=True
                            )
                            for doc in page_docs
                            if doc.get("sym") and doc.get("ti")
                        ]

                        if operations:
                            dest_collection.bulk_write(operations, ordered=False)

                        fetched = len(page_docs)
                        instrument_total += fetched
                        offset += fetched

                        if fetched < self.PAGE_SIZE:
                            break

                    logger.info(f"    ✓ {instrument}: {instrument_total} records loaded")
                    total_inserted += instrument_total

                except Exception as inst_error:
                    logger.error(f"  Failed to load daily candle for {instrument}: {inst_error}")
                    continue

            logger.info(f"✓ Daily candle preload complete: {total_inserted} total records loaded to '{dest_col}'")

        except Exception as e:
            logger.error(f"Fatal error in _pre_load_daily_candle: {e}")
            raise

    def _pre_load_expiry(self, source_db_name: str, source_collection_name: str, dest_col: str):
        """
        Loads expiry mapping from source database via Orb API
        and dumps to destination database under the specified collection.
        Only loads once per migration run.
        """
        if self.expiry_loaded:
            logger.info(f"   Expiry data already loaded, skipping...")
            return
            
        try:
            logger.info(f"   Loading expiry data from {source_db_name}.{source_collection_name}...")
            data = self.api_client.find(db=source_db_name, collection=source_collection_name, query={}) # type: ignore
            
            if data:
                dest_collection = self.dest_client[self.dest_db_name][dest_col] # type: ignore
                dest_collection.delete_many({})
                dest_collection.insert_many(data, ordered=False)
                logger.info(f"   ✓ Loaded {len(data)} expiry records to {dest_col}")
                self.expiry_loaded = True
            else:
                logger.warning(f"   No expiry data found in {source_db_name}.{source_collection_name}")
                
        except Exception as e:
            logger.error(f"   Failed to load expiry data: {e}")
            raise

    def _process_options(self, source_db_name: str, spot_source_db_name: str, dest_col, ti_data_col, instrument: str, year: int):
        """
        Process Options Data using the Orb DAL.
        - Spot data is fetched sequentially upfront (prerequisite for enrichment).
        - Options data is fetched in parallel across hourly time chunks.
        - Expiry cutoff cache is reset per instrument-year call.
        - The 'u' field query uses SYMBOL_MAPPING so config names like 'NIFTY'
          correctly resolve to 'NIFTY 50' in the source DB.
        """
        # Reset expiry cache for this instrument-year
        self._expiry_cutoff_cache = None

        ti_query = self.get_year_query(year)
        
        # Resolve the spot symbol — used for both spot query and options 'u' field
        spot_sym = self.SYMBOL_MAPPING.get(instrument, instrument)

        # 1. Fetch Spot Data for Enrichment (sequential — prerequisite for options enrichment)
        spot_query = {"sym": spot_sym, "ti": ti_query}
        
        logger.info("   Fetching spot docs for enrichment via API...")
        spot_docs = []
        offset = 0
        
        try:
            try:
                count = self.api_client.count(spot_source_db_name, str(year), spot_query) # type: ignore
                logger.info(f"   Found {count} spot documents to fetch")
            except Exception as count_error:
                logger.warning(f"   Could not count spot documents for {spot_source_db_name}.{year}: {count_error}")
            
            while True:
                try:
                    page_docs = self.api_client.find( # type: ignore
                        spot_source_db_name, str(year), spot_query, self.underlying_projection,
                        sort=[["ti", 1], ["sym", 1]],
                        limit=self.PAGE_SIZE, offset=offset
                    )
                    if not page_docs:
                        break
                    spot_docs.extend(page_docs)
                    offset += len(page_docs)
                    
                except Exception as find_error:
                    if "collection" in str(find_error).lower() and "not found" in str(find_error).lower():
                        logger.info(f"   Collection {spot_source_db_name}.{year} not found, skipping spot data")
                        break
                    else:
                        raise
                        
        except Exception as e:
            logger.warning(f"   Error fetching spot data: {e}")
        
        spot_dict = {}
        
        if spot_docs:
            logger.info(f"   Found {len(spot_docs)} spot documents via API")
            self._upload_spot_meta_batch(dest_col, spot_docs)
            spot_df = pl.DataFrame(spot_docs)
            for row in spot_df.iter_rows(named=True):
                ti_shifted = str(row["ti"] + 60)
                if ti_shifted not in spot_dict:
                    spot_dict[ti_shifted] = {}
                spot_dict[ti_shifted][row["sym"]] = {
                    k: row.get(k) for k in self.underlying_fields if row.get(k) is not None
                }
            logger.info(f"   ✓ Loaded {len(spot_docs)} spot data records for enrichment")

        # 2. Options Data via Parallel Hourly Time-Range Calls
        # SYMBOL_MAPPING ensures 'NIFTY' -> 'NIFTY 50' etc. for the 'u' field query
        opt_query = {"u": spot_sym}
        hourly_ranges = self._generate_hourly_ranges(year)
        
        logger.info(f"   Processing {len(hourly_ranges)} hourly chunks in parallel (MAX_WORKERS={self.MAX_WORKERS})...")
        
        with tqdm(total=len(hourly_ranges), desc=f"Migrating Options {instrument}-{year}", leave=False) as pbar:
            with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
                futures = {
                    executor.submit(
                        self._fetch_and_process_time_range,
                        source_db_name, year, opt_query, self.options_projection, 
                        start, end, dest_col, ti_data_col, spot_dict, False
                    ): (start, end) for start, end in hourly_ranges
                }
                
                for future in as_completed(futures):
                    count = future.result()
                    with self.lock:
                        self.stats['processed'] += count
                    pbar.update(1)

    def _process_spot(
        self,
        source_db_name: str,
        dest_col,
        ti_data_col,
        instrument: str,
        year: int,
        projection: Optional[Dict] = None,   # <-- None → falls back to underlying_projection
        fields: Optional[List[str]] = None,  # <-- None → falls back to underlying_fields
    ):
        """
        Process spot or futures instrument data using the Orb DAL.
        Uses hourly chunks for parallelism consistency with options processing.

        `projection` and `fields` allow callers (e.g. futures) to supply
        their own projection dict and field list without changing shared state.
        When omitted the method behaves exactly as before.
        """
        effective_projection = projection if projection is not None else self.underlying_projection
        effective_fields     = fields     if fields     is not None else self.underlying_fields

        query = {"u": instrument}
        hourly_ranges = self._generate_hourly_ranges(year)
        
        logger.info(f"   Processing {len(hourly_ranges)} hourly chunks in parallel (MAX_WORKERS={self.MAX_WORKERS})...")
        
        with tqdm(total=len(hourly_ranges), desc=f"Migrating Spot {instrument}-{year}", leave=False) as pbar:
            with ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
                futures = {
                    executor.submit(
                        self._fetch_and_process_time_range,
                        source_db_name, year, query, effective_projection,
                        start, end, dest_col, ti_data_col, None, True,
                        effective_fields,
                    ): (start, end) for start, end in hourly_ranges
                }

                for future in as_completed(futures):
                    count = future.result()
                    with self.lock:
                        self.stats['processed'] += count
                    pbar.update(1)

    def _fetch_and_process_spot_page_wrapper(self, db, year, query, proj, offset, dest_col, ti_data_col):
        """Wrapper to handle spot metadata + timeseries upload in parallel."""
        try:
            page_docs = self.api_client.find(db, str(year), query, proj, sort=[["ti", 1], ["sym", 1]], limit=self.PAGE_SIZE, offset=offset) # type: ignore
            if not page_docs: return 0
            
            self._upload_spot_meta_batch(dest_col, page_docs)
            
            rows = {}
            for doc in page_docs:
                ti = doc.get("ti")
                if not ti: continue
                ti_shifted = str(ti + 60)
                sym = doc.get("sym")
                if ti_shifted not in rows:
                    rows[ti_shifted] = {}
                rows[ti_shifted][sym] = {
                    k: doc.get(k) for k in self.underlying_fields if doc.get(k) is not None
                }
            
            if rows:
                u, m, ok = self._upload_data_batch(ti_data_col, rows)
                with self.lock:
                    if ok:
                        self.stats['uploaded'] += u
                        self.stats['modified'] += m
                    else:
                        self.stats['failed_batches'] += 1
            
            return len(page_docs)
        except Exception as e:
            logger.error(f"Error in spot worker: {e}")
            with self.lock:
                self.stats['failed_batches'] += 1
            return 0

    def run(self):
        """Main execution flow."""
        overall_start = time.time()
        
        try:
            self.configure_databases()
            self.connect()
            
            total_batches = len(self.global_batches)
            logger.info(f"\n{'=' * 60}")
            logger.info(f"▶ STARTING GLOBAL MIGRATION")
            logger.info(f"  Total batches: {total_batches}")
            logger.info(f"  Date range: {self.buffer_start_dt.strftime('%d-%m-%Y')} to {self.end_dt.strftime('%d-%m-%Y')}")
            logger.info(f"  (Includes {self.buffer_months} month buffer before {self.start_dt.strftime('%d-%m-%Y')})")
            logger.info(f"  Workers: {self.MAX_WORKERS} | Page size: {self.PAGE_SIZE:,}")
            logger.info(f"{'=' * 60}")

            if self.config.get('daily_candle_required'):
                self._pre_load_daily_candle(
                    source_db_name='eod_db',
                    source_col_name='Data',
                    dest_col='daily_candle'
                )
            
            for batch_info in self.global_batches:
                batch_num = batch_info['batch_num']
                db_groups = batch_info['db_groups']
                all_instruments = batch_info['all_instruments']
                
                ti_col_name = schema.get_data_collection_name(batch_num)
                ti_col = self.dest_db[ti_col_name] # type: ignore
                dest_col = self.data_instrument_collection
                
                self.batches_collection.update_one( # type: ignore
                    {"batch_name": ti_col_name},
                    {"$set": {
                        "batch_id": batch_num,
                        "instruments": all_instruments,
                        "updated_at": datetime.now(timezone.utc),
                        "count": len(all_instruments),
                        "date_range": {
                            "buffer_start": self.buffer_start_dt,
                            "config_start": self.start_dt,
                            "config_end": self.end_dt
                        }
                    }},
                    upsert=True
                )
                
                logger.info(f"\n{'=' * 60}")
                logger.info(f"→ BATCH {batch_num}/{total_batches}")
                logger.info(f"  Collections: {schema.DATA_INSTRUMENT_COLLECTION}, {ti_col_name}")
                logger.info(f"  Total instruments: {len(all_instruments)}")
                logger.info(f"  Processing years: {self.all_collection_years}")
                logger.info(f"{'=' * 60}")

                for db_name, info in db_groups.items():
                    instruments = info['instruments']
                    type_ = info['is_option']
                    
                    logger.info(f"\n  → Processing {db_name}: {len(instruments)} instruments")
                    
                    spot_db_name = None
                    if type_ == "options":
                        spot_db_name = self.config['spot_db_for_options'].get(db_name)
                    
                    for year in self.all_collection_years:
                        logger.info(f"\n    → Year: {year}")
                        
                        for inst in instruments:
                            logger.info(f"      • Processing: {inst}")
                            
                            if type_ == "options":
                                self._pre_load_expiry('FNO_Expiry', 'Data', 'FNO_Expiry')
                                self._process_options(db_name, spot_db_name, dest_col, ti_col, inst, year) # type: ignore
                            elif type_ == "future":
                                self._process_spot(
                                    db_name, dest_col, ti_col, inst, year,
                                    projection=self.futures_projection,
                                    fields=self.futures_fields,
                                )
                            else:
                                self._process_spot(db_name, dest_col, ti_col, inst, year)

        finally:
            overall_end = time.time()
            total_time = overall_end - overall_start
            
            self.disconnect()
            logger.info(f"\n{'=' * 60}")
            logger.info("✓ DATA MIGRATION COMPLETED!")
            logger.info(f"{'=' * 60}")
            logger.info(f"⏱ Total time: {total_time:.2f} seconds ({total_time / 60:.2f} minutes)")
            logger.info(f"Documents processed: {self.stats['processed']:,}")
            logger.info(f"New documents created: {self.stats['uploaded']:,}")
            logger.info(f"Existing documents modified: {self.stats['modified']:,}")
            logger.info(f"[X] Failed batches: {self.stats['failed_batches']}")
            logger.info(f"Date range with buffer: {self.buffer_start_dt.strftime('%d-%m-%Y')} to {self.end_dt.strftime('%d-%m-%Y')}")
            logger.info(f"{'=' * 60}")