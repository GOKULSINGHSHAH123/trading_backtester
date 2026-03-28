"""
Daily Pipeline Runner
=====================
Chains the full daily backtesting pipeline in sequence:

  1. backtester-migrate  — pull today's market data from Orb API → MongoDB
  2. backtester-run      — run the simulation (daily_mode auto-sets date to today)

Intended to be triggered once per day after market close (e.g. via cron):

    # crontab example — runs at 15:35 IST every weekday
    35 10 * * 1-5 cd /path/to/project && backtester-daily

Or called directly:
    python -m backtester.runner
    backtester-daily
"""

import sys
import logging

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    logger.info("=" * 60)
    logger.info("DAILY PIPELINE STARTING")
    logger.info("=" * 60)

    # ── Step 1: Data Migration ────────────────────────────────────────────────
    logger.info("\n[STEP 1/2] Running data migration (Orb API → MongoDB)...")
    try:
        from backtester.scripts.run_migration import main as run_migration
        run_migration()
        logger.info("[STEP 1/2] Migration complete.")
    except SystemExit as e:
        # run_migration calls sys.exit(1) on hard errors
        logger.error(f"[STEP 1/2] Migration failed (exit code {e.code}). Aborting pipeline.")
        sys.exit(e.code)
    except Exception as e:
        logger.error(f"[STEP 1/2] Migration raised an unexpected error: {e}")
        sys.exit(1)

    # ── Step 2: Simulation ────────────────────────────────────────────────────
    logger.info("\n[STEP 2/2] Running simulation...")
    try:
        from backtester.run_simulation import main as run_simulation
        run_simulation()
        logger.info("[STEP 2/2] Simulation complete.")
    except Exception as e:
        logger.error(f"[STEP 2/2] Simulation failed: {e}")
        sys.exit(1)

    logger.info("\n" + "=" * 60)
    logger.info("DAILY PIPELINE COMPLETE")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
