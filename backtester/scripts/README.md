# Data Preloading & Migration

## Overview

The Data Preloading system is an **offline ETL (Extract, Transform, Load) pipeline** designed to prepare raw market data for high-performance backtesting. 

It solves two critical problems:
1.  **Normalization**: Converting disparate raw data formats (Spot vs Options) into a unified schema.
2.  **Read Optimization**: Restructuring data from a "Symbol-First" format (typical in raw storage) to a "Time-First" format (`ti_data_X`) required for event-driven simulation.

**Crucially, the runtime backtester (`system.data_provider`) CANNOT function without this preloaded data.**

---

## Architecture & Data Flow

### 1. Source (Orb API)
The system connects to the **Orb Data Access Layer (DAL)**, a centralized API that abstracts the underlying storage of raw tick/OHLC data.
*   **Abstraction**: Allows the migrator to request data by time range and symbol without knowing the underlying database topology.
*   **Authentication**: Uses Client Credentials flow (Client ID + Secret).

### 2. Transformation Logic (`system.ingestion.migrator`)
The `MongoDBDataMigrator` class performs the following operations:

*   **Global Batching**: 
    *   It collects **ALL** instruments from all configured categories first.
    *   It creates global batches of 50 instruments (defined in `system.common.schema`). This ensures that `ti_data_{N}` collections are maximally packed, optimizing read throughput.
*   **Time-Partitioned Parallelism**:
    *   Unlike traditional pagination, the migrator splits the workload into **Daily Time Ranges** (e.g., `09:15-15:30` chunks).
    *   It processes these days in parallel using multiple threads, leveraging the database's time index for $O(\log N)$ retrieval speed instead of slow offset scanning.
*   **Dynamic Projection**: 
    *   The migrator reads `underlying_projection` and `options_projection` from `migration_config.yml`.
    *   It strictly projects only the configured fields into the destination database.
*   **Expiry Filtering**: For options, it filters out contracts that are not relevant (e.g., keeping only those expiring within N days).
*   **Enrichment (The "Spot" Lookup)**: 
    *   When processing Options, the system *simultaneously* fetches the underlying Spot price.
    *   It injects this Spot price into the same time-bucket.
    *   *Why?* Strategies often need to check the Spot price to decide on Option entry. Pre-joining this data avoids expensive lookups during the runtime loop.

### 3. Destination (Backtest-Ready Data)
The data is written to the destination MongoDB (defined in `migration_config.yml`) in three specific collection types:

| Collection Name | Purpose | Structure |
| :--- | :--- | :--- |
| **`data_instrument`** | **Symbol Lookup** | Stores flat OHLCV data per symbol. Used by the DataProvider to load historical context (e.g., for calculating Indicators like RSI/EMA) *before* the simulation starts. |
| **`ti_data_{N}`** | **Time-Interval Batches** | Stores the "Entire Market" state for a specific timestamp. <br> `{ "_id": <timestamp>, "NIFTY": {...}, "RELIANCE": {...} }`. <br> Used for the main event loop streaming. |
| **`batches`** | **Metadata** | Tracks which instruments belong to which `ti_data_{N}` collection. |

---

## Configuration

The process is controlled by `configs/migration_config.yml`:

```yaml
# Destination DB Name
db_name: "data50"

# Date Range to process
starting_date: "01-01-2024"
ending_date: "31-12-2024"

# Fields to Keep (1 = Keep, 0 = Ignore)
underlying_projection:
  sym: 1
  ti: 1
  o: 1
  h: 1
  l: 1
  c: 1
options_projection:
  sym: 1
  ti: 1
  o: 1
  h: 1
  l: 1
  c: 1
  oi: 1 # Example: Add Open Interest for Options
```

**Secrets** are loaded from the `.env` file in the project root:
```bash
# Destination (Your Local/Cloud MongoDB)
DEST_DB_URI="mongodb://admin:password@localhost:27017/"

# Source (Orb DAL API Credentials)
ORB_API_URL="http://localhost:8000"
ORB_CLIENT_ID="your_client_id"
ORB_CLIENT_SECRET="your_client_secret"
```

---

## Usage Guide

### How to Preload Data for Your Backtest

This guide details the steps to configure and run the data preloading process.

#### **Step 1: Environment Setup**

1.  **Create `.env` File**:
    *   In the root directory of your project, create a file named `.env`.
    *   Add your MongoDB destination URI and Orb API credentials.
    ```dotenv
    DEST_DB_URI="mongodb://admin:password@localhost:27017/"
    ORB_API_URL="http://10.0.0.1:8000"
    ORB_CLIENT_ID="my_user"
    ORB_CLIENT_SECRET="my_secret_key"
    ```

#### **Step 2: Configure the Migration (`configs/migration_config.yml`)**

This file controls what data to extract, for which period, and how to shape it.

1.  **Open `configs/migration_config.yml`**:
    *   **`db_name` (Destination Database)**: Name of the database on your `DEST_DB_URI` server where the processed data will reside. E.g., `db_name: "backtest_data_2024"`
    *   **`starting_date` & `ending_date` (Date Range)**: Defines the historical period for data extraction (`DD-MM-YYYY`).
    *   **`expiry_days` (Options Data Filtering)**: For options, only contracts expiring within this many days will be processed.
    *   **Instrument Lists**: Populate `stock`, `index_options`, etc., with the specific symbols you want to backtest.

#### **Step 3: Run the Migration Script**

Execute the script from your project's root directory.

```bash
python scripts/run_migration.py
```

#### **Step 4: Monitor and Verify**

1.  **Terminal Output**: Observe detailed logs. You should see "Processing X days in parallel..." indicating the new optimized workflow.
2.  **Database Inspection**: Use a MongoDB client to verify the data landed in the expected `ti_data_{N}` collections.

---

## System Dependency

The Runtime Backtester (`run_simulation.py`) depends on this data structure:

1.  **Initialization**: `BaseDataProvider` reads `data_instrument` to build indicator history.
2.  **Streaming**: `BaseDataProvider` connects to `ti_data_{N}` to stream market packets.

**If you change the schema in `system.common.schema` or `migration_config.yml`'s projection, you MUST re-run the migration.**