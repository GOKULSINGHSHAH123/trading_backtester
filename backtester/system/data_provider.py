from pymongo import MongoClient # type: ignore
import polars as pl # type: ignore
import time
from tqdm import tqdm # type: ignore
import gc
import psutil # type: ignore
import os
import logging
import multiprocessing as mp
from multiprocessing import Pool, cpu_count
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from typing import Callable, List
from backtester.system.common import schema


class BaseDataProvider:

    SYMBOL_MAPPING = {
        "NIFTY 50": "NIFTY",
        "NIFTY BANK": "BANKNIFTY",
    }

    def __init__(self, config):
        """Initialize DataProvider with configuration."""
        self.logger = logging.getLogger(__name__)
        try:
            mp.set_start_method('spawn', force=True)
        except RuntimeError:
            pass

        self.config = config

        # Database Configuration (Injected via config_loader)
        self.db_uri = config.get("db_uri")
        self.db_name = config.get("db_name")
        self.is_commodity_futures = config.get("is_commodity_futures", False)

        if not self.db_uri:
            raise ValueError("Database URI not provided in config (check .env and simulation_config.yml)")

        # ── BUFFER / WARMUP ──────────────────────────────────────────────────
        # buffer_days: how many calendar days of extra history to load BEFORE
        # starting_ti so that indicators & prev_day_candle are pre-seeded.
        # Set buffer_days: 0 in config to disable (old behaviour).
        self.buffer_days = int(config.get("buffer_days", 60))

        # Real simulation boundary (unchanged — strategies never see warmup data)
        self.starting_ti  = self._parse_date(config["starting_ti_year"])
        self.ending_ti    = self._parse_date(config["ending_ti_year"])

        # Extended load boundary = starting_ti minus buffer
        self.buffered_starting_ti = self.starting_ti - self.buffer_days * 86400
        # ─────────────────────────────────────────────────────────────────────

        self.all_instruments = config["instruments"]

        # Load simple timeframe list
        self.resample_intervals = config["timeframes"]

        # Dynamic Timeframe Conversion
        self.TIMEFRAME_SECONDS = {
            tf: self._parse_timeframe_to_seconds(tf) for tf in self.resample_intervals
        }

        # Schema Constants
        self.max_instruments_per_collection = schema.MAX_INSTRUMENTS_PER_COLLECTION
        self.collection_base_name = schema.COLLECTION_BASE_NAME

        self.instrument_batches = self._split_instruments_into_batches()
        self.total_collections = len(self.instrument_batches)

        self.current_collection_num = None
        self.current_instruments = []
        self.all_instruments_data = {}
        self.timeframe_ts_set = {}
        self.timestamp_index = {}
        self.num_processes = max(1, cpu_count() - 1)
        self.expiry_collection = "FNO_Expiry"

        self.client = None
        self.collection = None

        self.overall_start_time = None
        self.collection_stats = {}
        self.total_packets_processed = 0

        # List of transform functions registered by strategies
        self.transforms: List[Callable] = []
        self.global_transforms: List[Callable] = []

        # Daily candle index: { 1m_timestamp -> { sym -> {o,h,l,c,v,...} } }
        self.daily_candle_index = {}

        self.logger.info(f"DataProvider Initialized")
        self.logger.info(f"Date range: {config['starting_ti_year']} to {config['ending_ti_year']}")
        self.logger.info(f"Buffer days: {self.buffer_days}  "
                         f"(loading from {datetime.utcfromtimestamp(self.buffered_starting_ti).date()})")
        self.logger.info(f"Total instruments: {len(self.all_instruments)}")
        self.logger.info(f"Total collections: {self.total_collections}")
        self.logger.info(f"Timeframes: {self.resample_intervals}")
        self.logger.info(f"Processing cores: {self.num_processes}\n")

    # ─────────────────────────────────────────────────────────────────────────
    # Unchanged helpers
    # ─────────────────────────────────────────────────────────────────────────

    def _parse_timeframe_to_seconds(self, tf_str: str) -> int:
        """Parses a timeframe string (e.g., '1m', '5m', '1h') to seconds."""
        if not tf_str:
            return 0
        unit = tf_str[-1].lower()
        if not unit.isalpha():
            raise ValueError(f"Invalid timeframe format: {tf_str}. Must end with a unit (m, h, d, w).")
        try:
            value = int(tf_str[:-1])
        except ValueError:
            raise ValueError(f"Invalid timeframe format: {tf_str}. Could not parse numeric value.")
        multipliers = {'m': 60, 'h': 3600, 'd': 86400, 'w': 604800}
        if unit not in multipliers:
            raise ValueError(f"Unknown timeframe unit '{unit}' in '{tf_str}'. Supported: {list(multipliers.keys())}")
        return value * multipliers[unit]

    def register_transform(self, func: Callable):
        self.transforms.append(func)

    def register_global_transform(self, func: Callable):
        self.global_transforms.append(func)

    def _split_instruments_into_batches(self):
        batches = {}
        for i in range(0, len(self.all_instruments), self.max_instruments_per_collection):
            batch_num = schema.get_batch_num_from_index(i)
            batch = self.all_instruments[i:i + self.max_instruments_per_collection]
            batches[batch_num] = batch
        return batches

    def get_collection_numbers(self):
        return sorted(self.instrument_batches.keys())

    # ─────────────────────────────────────────────────────────────────────────
    # DB loaders — now use buffered_starting_ti instead of starting_ti
    # ─────────────────────────────────────────────────────────────────────────

    def load_data_by_sym(self, symbol: str) -> pl.DataFrame:
        """Loads 1m data for a single symbol. Uses buffered start to pre-seed indicators."""
        client = None
        try:
            client = MongoClient(self.db_uri, serverSelectionTimeoutMS=20000, socketTimeoutMS=20000)
            collection = client[self.db_name][schema.DATA_INSTRUMENT_COLLECTION]
            print(self.db_name)
            print(schema.DATA_INSTRUMENT_COLLECTION)
            
            query = {
                "sym": symbol,
                "ti": {
                    "$gte": self.buffered_starting_ti,   # ← CHANGED (was starting_ti)
                    "$lt":  self.ending_ti,
                }
            }
            print(query)
            cursor = collection.find(query, {"_id": 0}).sort("ti", 1).batch_size(50_000)
            data = list(cursor)
            if not data:
                return pl.DataFrame()
            df = pl.DataFrame(data)
            return df.sort("ti").with_columns(pl.col("ti").set_sorted())
        except Exception as e:
            self.logger.error(f"Error loading {symbol}: {e}")
            return pl.DataFrame()
        finally:
            if client:
                client.close()

    def load_daily_candle_by_sym(self, symbol: str) -> pl.DataFrame:
        """
        Loads daily candle data. Uses buffered start so prev_day_candle is
        available from day-1 of the real simulation window.
        """
        client = None
        try:
            client = MongoClient(self.db_uri, serverSelectionTimeoutMS=20000, socketTimeoutMS=20000)
            collection = client[self.db_name][schema.daily_candle]
            query = {
                "sym": symbol,
                "ti": {
                    "$gte": self.buffered_starting_ti,   # ← CHANGED (was starting_ti)
                    "$lte": self.ending_ti,
                }
            }
            cursor = collection.find(query, {"_id": 0}).sort("ti", 1).batch_size(50_000)
            data = list(cursor)
            if not data:
                return pl.DataFrame()
            for doc in data:
                doc.pop("sym", None)
            df = pl.from_dicts(data, infer_schema_length=None)
            if "ti" not in df.columns:
                return pl.DataFrame()
            return df.sort("ti").with_columns(pl.col("ti").set_sorted())
        except Exception as e:
            self.logger.error(f"Error loading daily candle for {symbol}: {e}")
            return pl.DataFrame()
        finally:
            if client:
                client.close()

    def load_expiry(self, symbols=None):
        """Loads expiry data for multiple symbols and expands to daily rows."""
        if symbols is None:
            symbols = ['NIFTY', 'SENSEX', 'ACC', 'BANKNIFTY']
        client = None
        try:
            client = MongoClient(self.db_uri, serverSelectionTimeoutMS=20000, socketTimeoutMS=20000)
            collection = client[self.db_name][self.expiry_collection]
            cursor = collection.find({"Sym": {"$in": symbols}}, {"_id": 0}).sort("Date", 1)
            data = list(cursor)
            if not data:
                print(f"No data found for symbols: {symbols}")
                return pl.DataFrame()

            df = pl.from_dicts(data, infer_schema_length=None)
            if 'Date' in df.columns:
                df = df.with_columns(pl.col("Date").str.to_date("%Y-%m-%d", strict=False))
            
            # Identify all columns except 'Date', 'Sym', and 'LastUpdated' to propagate
            df_schema = df.schema
            propagate_cols = [c for c in df.columns if c not in ['Date', 'Sym', 'LastUpdated']]

            # Simulation start anchor
            sim_start_date = datetime.fromtimestamp(self.buffered_starting_ti, tz=timezone.utc).date()

            expanded_dfs = []
            for symbol in symbols:
                symbol_df = df.filter(pl.col('Sym') == symbol).sort("Date")
                if symbol_df.height == 0:
                    print(f"No data found for symbol: {symbol}")
                    continue
                
                # Use unique dates, keeping the last record for that day
                symbol_df = symbol_df.unique(subset=['Date'], keep='last', maintain_order=True)
                
                # Derive StartDate from the previous row's Date
                symbol_df = symbol_df.with_columns([pl.col('Date').shift(1).alias('StartDate')])
                
                # Anchor the very first row to the simulation start date
                symbol_df = symbol_df.with_columns([
                    pl.col('StartDate').fill_null(sim_start_date)
                ])
                
                expanded_rows = []
                for row in symbol_df.to_dicts():
                    start_date = row['StartDate']
                    end_date = row['Date']
                    
                    if start_date is None or end_date is None:
                        continue
                
                # To restore enumerate correctly while ensuring native types:
                symbol_dicts = symbol_df.to_dicts()
                for i, row in enumerate(symbol_dicts):
                    start_date = row['StartDate']
                    end_date = row['Date']
                    
                    if start_date is None or end_date is None:
                        continue
                        
                    if i > 0:
                        start_date = start_date + timedelta(days=1)
                    
                    if start_date > end_date:
                        continue

                    date_range = pl.DataFrame({
                        'Date': pl.date_range(start_date, end_date, interval='1d', eager=True)
                    })
                    date_range = date_range.with_columns([
                        pl.lit(symbol).alias('Sym'),
                        *[pl.lit(row[c], dtype=df_schema[c]).alias(c) for c in propagate_cols]
                    ])
                    expanded_rows.append(date_range)
                if expanded_rows:
                    expanded_dfs.append(pl.concat(expanded_rows, how='vertical'))
            if not expanded_dfs:
                print("No expanded data created for any symbols")
                return pl.DataFrame()
            result = pl.concat(expanded_dfs, how='vertical')
            return result.sort(['Sym', 'Date']).with_columns(pl.col("Date").set_sorted())
        finally:
            if client:
                client.close()

    # ─────────────────────────────────────────────────────────────────────────
    # Resampling — unchanged
    # ─────────────────────────────────────────────────────────────────────────

    def resample_ohlcv(self, df: pl.DataFrame, intervals: list[str]) -> dict[str, pl.DataFrame]:
        """
        Resamples 1m OHLCV data into requested intervals.
        NOTE: '1d' is intentionally skipped here — loaded directly from DB.
        Buffer rows are included so indicators warm up correctly.
        """
        df = df.with_columns(pl.from_epoch(pl.col("ti"), time_unit="s").alias("datetime"))
        resampled_data = {}
        has_open   = "o" in df.columns
        has_high   = "h" in df.columns
        has_low    = "l" in df.columns
        has_close  = "c" in df.columns
        has_volume = "v" in df.columns
        # Dynamically identify expiry/extra columns (anything not standard OHLCV or internal)
        standard_cols = {"ti", "o", "h", "l", "c", "v", "datetime", "ist_dt", "Date", "Sym", "Expiry_Sym"}
        expiry_columns = [col for col in df.columns if col not in standard_cols]

        # Define market hours based on segment
        if self.is_commodity_futures:
            mkt_start = pl.time(9, 0)
            mkt_end = pl.time(23, 29)
            mkt_offset = "9h"
        else:
            mkt_start = pl.time(9, 15)
            mkt_end = pl.time(15, 29)
            mkt_offset = "9h15m"

        for interval in intervals:
            if interval == "1d":
                continue

            aggs = [(pl.col("datetime").first().dt.timestamp("ms") // 1000).alias("ti")]

            if has_open:
                aggs.append(pl.col("o").first().alias("o"))
            else:
                aggs.append(pl.col("c").first().alias("o") if has_close else pl.lit(None).alias("o"))
            if has_high:
                aggs.append(pl.col("h").max().alias("h"))
            else:
                aggs.append(pl.col("c").max().alias("h") if has_close else pl.lit(None).alias("h"))
            if has_low:
                aggs.append(pl.col("l").min().alias("l"))
            else:
                aggs.append(pl.col("c").min().alias("l") if has_close else pl.lit(None).alias("l"))
            if has_close:
                aggs.append(pl.col("c").last().alias("c"))
            else:
                aggs.append(pl.lit(None).alias("c"))
            if has_close and (not has_open or not has_high or not has_low):
                self.logger.warning(f"OHLC for interval {interval} derived from Close due to missing O/H/L")
            if has_volume:
                aggs.append(pl.col("v").sum().alias("v"))
            for col in expiry_columns:
                aggs.append(pl.col(col).first().alias(col))

            out = (
                df
                .with_columns(
                    ist_dt=(
                        pl.col("datetime")
                        .dt.replace_time_zone("UTC")
                        .dt.convert_time_zone("Asia/Kolkata")
                    )
                )
                .filter(
                    (pl.col("ist_dt").dt.time() >= mkt_start) &
                    (pl.col("ist_dt").dt.time() <= mkt_end)
                )
                .group_by_dynamic("ist_dt", every=interval, offset=mkt_offset, closed="left", label="left")
                .agg(aggs)
                .drop("ist_dt")
                .sort("ti")
                .with_columns(pl.col("ti").set_sorted())
            )
            resampled_data[interval] = out

        return resampled_data

    # ─────────────────────────────────────────────────────────────────────────
    # Static / misc helpers — unchanged
    # ─────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_date(date_str):
        dt = datetime.strptime(date_str, "%d-%m-%Y")
        return int(dt.timestamp())

    @staticmethod
    def get_memory_usage():
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024

    @staticmethod
    def force_garbage_collection():
        gc.collect(2)

    def _process_data(self, df):
        if df.is_empty() or "sym" not in df.columns:
            return df
        df = df.with_columns([
            pl.col("sym").replace(BaseDataProvider.SYMBOL_MAPPING, default=pl.col("sym"))
        ])
        df = df.rename({"sym": "Sym"})
        df = df.with_columns([
            pl.from_epoch("ti", time_unit="s").cast(pl.Date).alias("Date")
        ])
        return df

    def _process_expiry(self, df):
        df = df.with_columns(
            pl.when(pl.col('Sym').is_in(['BANKNIFTY', 'NIFTY', 'SENSEX']))
            .then(pl.col('Sym'))
            .otherwise(pl.lit('ACC'))
            .alias('Expiry_Sym')
        )
        expiry_df = self.load_expiry()
        df = df.join(expiry_df, left_on=['Date', 'Expiry_Sym'], right_on=['Date', 'Sym'], how='left')
        df = df.drop("Expiry_Sym")
        return df

    def apply_data_transform_func(self, resample_intervals, resampled_results, transforms, instrument):
        for interval in resample_intervals:
            if interval not in resampled_results:
                continue
            data = resampled_results[interval]
            for transform_func in transforms:
                try:
                    data = transform_func(data, interval)
                except Exception as e:
                    self.logger.error(f"Error applying transform {transform_func.__name__} for {instrument}: {e}")
            resampled_results[interval] = data
        return resampled_results

    def _process_single_instrument(self, args):
        instrument, resample_intervals, transforms = args
        try:
            df = self.load_data_by_sym(instrument)          # now loads buffered window
            if df.is_empty():
                self.logger.warning(f"No data found for {instrument}, skipping.")
                return (instrument, {})
            df = self._process_data(df)
            if df.is_empty():
                self.logger.warning(f"Empty DataFrame after processing {instrument}, skipping.")
                return (instrument, {})
            if self.config['is_options_segment']:
                df = self._process_expiry(df)

            resampled_results = self.resample_ohlcv(df, resample_intervals)
            del df

            # if "1d" in resample_intervals:
            daily_df = self.load_daily_candle_by_sym(instrument)  # buffered window
            if not daily_df.is_empty():
                resampled_results["1d"] = daily_df
            else:
                self.logger.warning(f"No daily candle data found for {instrument}, skipping 1d.")

            resampled_results = self.apply_data_transform_func(
                resample_intervals, resampled_results, transforms, instrument
            )
            return (instrument, resampled_results)
        except Exception as e:
            self.logger.error(f"Error processing {instrument}: {e}")
            return (instrument, {})

    @staticmethod
    def _convert_to_minimal_dict_fast(args):
        symbol, timeframes = args
        timestamp_dict = {}
        try:
            for timeframe, df in timeframes.items():
                rows = df.to_dicts()
                tf_dict = {}
                for row in rows:
                    ti = row.pop("ti")
                    clean_row = {k: v for k, v in row.items() if v is not None}
                    tf_dict[ti] = clean_row
                timestamp_dict[timeframe] = tf_dict
                del df
                gc.collect()
            return symbol, timestamp_dict
        except Exception as e:
            logging.getLogger(__name__).error(f"Error converting {symbol}: {e}")
            return symbol, {}

    def _parallel_process_instruments(self):
        self.logger.info(f"Processing {len(self.current_instruments)} instruments...")
        args = [(inst, self.resample_intervals, self.transforms) for inst in self.current_instruments]
        with Pool(processes=self.num_processes) as pool:
            results = list(tqdm(
                pool.imap(self._process_single_instrument, args),
                total=len(self.current_instruments),
                desc=f"Processing collection {self.current_collection_num}"
            ))
        all_instruments_data = {instrument: data for instrument, data in results if data}
        self.force_garbage_collection()
        return all_instruments_data

    def _build_timeframe_timestamp_sets(self):
        timeframe_ts_set = {}
        if not self.all_instruments_data:
            return timeframe_ts_set
        ref_symbol = next(iter(self.all_instruments_data))
        for timeframe, df in self.all_instruments_data[ref_symbol].items():
            timeframe_ts_set[timeframe] = frozenset(df["ti"].to_numpy())
        return timeframe_ts_set

    def _parallel_convert_to_timestamp_dict(self):
        self.logger.info("Converting to minimal dict format...")
        items = list(self.all_instruments_data.items())
        with Pool(processes=self.num_processes) as pool:
            results = list(tqdm(
                pool.imap(self._convert_to_minimal_dict_fast, items),
                total=len(items),
                desc="Converting to dicts"
            ))
        timestamp_dict = {symbol: data for symbol, data in results if data}
        self.all_instruments_data.clear()
        self.force_garbage_collection()
        return timestamp_dict

    # ─────────────────────────────────────────────────────────────────────────
    # Timestamp index — now aware of warmup boundary
    # ─────────────────────────────────────────────────────────────────────────

    def _build_timestamp_index(self):
        """
        Builds the timestamp index covering the FULL loaded window
        (buffered_starting_ti → ending_ti).

        stream() will silently skip packets whose timestamp < starting_ti
        so strategies never receive warmup ticks. The index must cover
        warmup ticks so that indicator state is built up correctly if a
        strategy inspects all_instruments_data directly during transforms.

        NOTE: In practice stream() skips warmup docs at the cursor level,
        so this index is only queried for real ticks. Building it over the
        full window costs a bit of RAM but keeps logic identical to before.
        """
        self.logger.info("Building timestamp index...")
        self.timestamp_index = defaultdict(list)
        if not self.all_instruments_data:
            return

        global_timeframe_timestamps = defaultdict(set)
        for symbol_data in self.all_instruments_data.values():
            for tf, tf_data in symbol_data.items():
                if tf in self.TIMEFRAME_SECONDS:
                    global_timeframe_timestamps[tf].update(tf_data.keys())

        clock_symbols = ["NIFTY 50", "NIFTY", "NIFTY BANK", "BANKNIFTY", "SENSEX"]
        master_clock_symbol = None
        for sym in clock_symbols:
            if sym in self.all_instruments_data and "1m" in self.all_instruments_data[sym]:
                master_clock_symbol = sym
                self.logger.info(f"Using '{sym}' as Master Clock for simulation steps.")
                break

        if master_clock_symbol:
            global_1m_timestamps = set(self.all_instruments_data[master_clock_symbol]["1m"].keys())
        else:
            self.logger.info("No Index found. Building Master Clock from union of all instruments (slower)...")
            global_1m_timestamps = global_timeframe_timestamps.get("1m", set())

        if not global_1m_timestamps:
            self.logger.warning("No 1m data found to build master clock.")
            return

        sorted_1m_timestamps = sorted(global_1m_timestamps)

        for current_ts in tqdm(sorted_1m_timestamps, desc="Building index"):
            one_min_ts = current_ts - self.TIMEFRAME_SECONDS["1m"]
            if one_min_ts in global_timeframe_timestamps.get("1m", set()):
                self.timestamp_index[current_ts].append(("1m", one_min_ts))

            for timeframe in self.resample_intervals:
                if timeframe == "1m" or timeframe == "1d":
                    continue
                tf_seconds = self.TIMEFRAME_SECONDS.get(timeframe)
                if not tf_seconds:
                    continue
                target_ts = current_ts - tf_seconds
                if target_ts in global_timeframe_timestamps.get(timeframe, set()):
                    self.timestamp_index[current_ts].append((timeframe, target_ts))

        self.logger.info(f"Timestamp index built: {len(self.timestamp_index)} entries")

    # ─────────────────────────────────────────────────────────────────────────
    # Daily candle index — unchanged logic, but now has buffer rows available
    # ─────────────────────────────────────────────────────────────────────────

    def _build_daily_candle_index(self):
        """
        For every 1m timestamp (full buffered window), find the most recent
        completed daily candle strictly from the previous date.
        Because daily candles now start from buffered_starting_ti, the very
        first real simulation day already has a valid prev_day_candle.
        """
        # if "1d" not in self.resample_intervals:
        #     self.daily_candle_index = {}
        #     return

        # self.logger.info("Building daily candle index from all_instruments_data['1d']...")

        IST = timezone(timedelta(hours=5, minutes=30))

        def to_date_int(ts):
            dt = datetime.fromtimestamp(ts, tz=IST)
            return dt.year * 10000 + dt.month * 100 + dt.day

        all_1m_ts = sorted(self.timestamp_index.keys())
        self.daily_candle_index = {}

        for sym, sym_data in self.all_instruments_data.items():
            if "1d" not in sym_data:
                continue
            sorted_daily_ts = sorted(sym_data["1d"].keys())
            if not sorted_daily_ts:
                continue
            candle_idx = 0
            for ts in all_1m_ts:
                ts_date = to_date_int(ts)
                while (
                    candle_idx + 1 < len(sorted_daily_ts)
                    and to_date_int(sorted_daily_ts[candle_idx + 1]) < ts_date
                ):
                    candle_idx += 1
                prev_ts = sorted_daily_ts[candle_idx]
                if to_date_int(prev_ts) >= ts_date:
                    continue
                if ts not in self.daily_candle_index:
                    self.daily_candle_index[ts] = {}
                self.daily_candle_index[ts][sym] = {"timestamp": prev_ts, **sym_data["1d"][prev_ts]}

        self.logger.info(f"✓ Daily candle index built: {len(self.daily_candle_index)} timestamps covered")

    def _apply_global_transformation(self):
        self.logger.info(f"Applying {len(self.global_transforms)} global transforms...")
        for func in self.global_transforms:
            try:
                self.all_instruments_data = func(self.all_instruments_data)
            except Exception as e:
                import traceback
                traceback.print_exc()
                self.logger.error(f"Global transform failed: {e}")
        self.current_instruments = sorted(list(self.all_instruments_data.keys()))
        self.logger.info(f"Instruments after global transform: {len(self.current_instruments)}")

    # ─────────────────────────────────────────────────────────────────────────
    # initialize — unchanged
    # ─────────────────────────────────────────────────────────────────────────

    def initialize(self, collection_num):
        if self.overall_start_time is None:
            self.overall_start_time = time.time()
        collection_start_time = time.time()
        self.current_collection_num = collection_num
        self.current_instruments = self.instrument_batches[collection_num]
        self.logger.info(f"\nInitializing Collection {collection_num}/{self.total_collections}")

        self.all_instruments_data = self._parallel_process_instruments()

        if self.global_transforms and self.all_instruments_data:
            self._apply_global_transformation()

        self.timeframe_ts_set = self._build_timeframe_timestamp_sets()
        self.all_instruments_data = self._parallel_convert_to_timestamp_dict()
        self._build_timestamp_index()
        self._build_daily_candle_index()

        init_time = time.time() - collection_start_time
        self.collection_stats[collection_num] = {'init_time': init_time, 'start_time': time.time()}

    # ─────────────────────────────────────────────────────────────────────────
    # Packet generator — unchanged
    # ─────────────────────────────────────────────────────────────────────────

    def _generate_packets_fast(self, current_timestamp):
        timeframe_hits = self.timestamp_index.get(int(current_timestamp), [])
        if not timeframe_hits:
            return {}

        packet = {'timestamp': current_timestamp}

        for timeframe, target_timestamp in timeframe_hits:
            current_data = {
                s: self.all_instruments_data[s][timeframe].get(target_timestamp)
                for s in self.current_instruments
                if s in self.all_instruments_data
                and timeframe in self.all_instruments_data[s]
                and self.all_instruments_data[s][timeframe].get(target_timestamp)
            }
            if current_data:
                packet[timeframe] = {'current_data': current_data}

        prev_day_data = self.daily_candle_index.get(int(current_timestamp))
        if prev_day_data:
            packet['prev_day_candle'] = prev_day_data

        if len(packet) <= 1:
            return {}
        return packet

    # ─────────────────────────────────────────────────────────────────────────
    # stream — ONLY change: skip warmup ticks (timestamp < starting_ti)
    # ─────────────────────────────────────────────────────────────────────────

    def stream(self):
        """
        Streams real simulation ticks only (timestamp >= starting_ti).
        Warmup ticks (buffered_starting_ti … starting_ti) are silently skipped
        at the DB cursor level — they never reach the strategy.
        Indicators and prev_day_candle are fully seeded by the time the first
        real tick is yielded.
        """
        if self.current_collection_num is None:
            raise RuntimeError("Must call initialize(collection_num) before streaming!")
        streaming_client = None
        try:
            streaming_client = MongoClient(self.db_uri)
            db = streaming_client[self.db_name]

            collection_name = schema.get_data_collection_name(self.current_collection_num)
            collection = db[collection_name]

            # ── Real window only: skip warmup at query level ──────────────────
            query_filter = {
                "_id": {
                    "$gte": self.starting_ti,    # ← CHANGED (was buffered start)
                    "$lte": self.ending_ti,
                }
            }
            total_docs = collection.count_documents(query_filter)
            use_ti_field = total_docs == 0
            if use_ti_field:
                query_filter = {
                    "ti": {
                        "$gte": self.starting_ti,   # ← CHANGED
                        "$lte": self.ending_ti,
                    }
                }
                total_docs = collection.count_documents(query_filter)
            if total_docs == 0:
                return
            # ─────────────────────────────────────────────────────────────────

            cursor_sort_key = "ti" if use_ti_field else "_id"
            cursor = collection.find(query_filter).sort(cursor_sort_key, 1).batch_size(50_000)

            processed_count, packet_count = 0, 0

            for doc in tqdm(cursor, total=total_docs, desc=f"Streaming collection {self.current_collection_num}"):
                timestamp = int(doc.get("ti", doc.get("_id")))

                processed_count += 1
                packet = self._generate_packets_fast(timestamp)
                if packet:
                    packet_count += 1
                yield packet, doc

            stream_time = time.time() - self.collection_stats[self.current_collection_num]['start_time']
            self.collection_stats[self.current_collection_num].update({
                'packets': packet_count,
                'stream_time': stream_time,
                'processed_count': processed_count,
            })

        finally:
            if streaming_client:
                streaming_client.close()

    # ─────────────────────────────────────────────────────────────────────────
    # Cleanup / misc — unchanged
    # ─────────────────────────────────────────────────────────────────────────

    def clear_memory(self):
        self.logger.info(f"Clearing memory for collection {self.current_collection_num}...")
        self.all_instruments_data.clear()
        self.timeframe_ts_set.clear()
        self.timestamp_index.clear()
        self.daily_candle_index.clear()
        self.current_instruments.clear()
        self.force_garbage_collection()

    def print_summary(self):
        if self.overall_start_time is None:
            return
        total_time = time.time() - self.overall_start_time
        self.logger.info(f"\nOVERALL SUMMARY")
        self.logger.info(f"Total time: {total_time:.2f}s ({total_time/60:.2f} minutes)")

    def close(self):
        if self.client:
            self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()