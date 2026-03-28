import os
import yaml
import sys
import logging
import argparse
from dotenv import load_dotenv # type: ignore
from pathlib import Path

from backtester.system.ingestion.migrator import MongoDBDataMigrator

def main():
    parser = argparse.ArgumentParser(description="Migrate market data into the backtester database.")
    parser.add_argument(
        "--config",
        type=str,
        default="migrator/migration_config.yml",
        help="Path to the migration configuration file (default: migrator/migration_config.yml)."
    )
    args = parser.parse_args()

    # Load .env from current working directory or parent directories
    load_dotenv()

    dest_uri = os.getenv("DEST_DB_URI")
    orb_api_url = os.getenv("ORB_API_URL")
    orb_client_id = os.getenv("ORB_CLIENT_ID")
    orb_client_secret = os.getenv("ORB_CLIENT_SECRET")

    if not dest_uri:
        print("[X] Error: DEST_DB_URI must be set in .env")
        sys.exit(1)

    if not all([orb_api_url, orb_client_id, orb_client_secret]):
        print("[X] Error: ORB_API_URL, ORB_CLIENT_ID, and ORB_CLIENT_SECRET must be set in .env")
        sys.exit(1)

    # Load Configuration from the provided path
    config_path = Path(args.config)
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"[X] Error loading config from '{config_path}': {e}")
        sys.exit(1)

    # Initialize and Run Migrator
    try:
        print(f"[--] Starting Migration Service via Orb DAL")
        migrator = MongoDBDataMigrator(
            config, 
            dest_uri, 
            orb_api_url, # type: ignore
            orb_client_id, # type: ignore
            orb_client_secret # type: ignore
        )
        migrator.run()
    except KeyboardInterrupt:
        print("\n [^] Interrupted by user")
    except Exception as e:
        logging.exception("Fatal error during migration")
        sys.exit(1)

if __name__ == "__main__":
    main()
