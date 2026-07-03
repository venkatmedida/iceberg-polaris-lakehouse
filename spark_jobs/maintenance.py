#!/usr/bin/env python3
"""
Iceberg table maintenance via Spark stored procedures.

Operations (run in order — compaction first, then cleanup):
  1. rewrite_data_files  — bin-pack small files into target-size files
  2. rewrite_manifests   — consolidate manifest files after many writes
  3. expire_snapshots    — remove snapshots older than the retention window
  4. remove_orphan_files — delete unreferenced files left by failed writes

Run on a schedule (e.g., daily cron or Airflow DAG) after high-throughput
ingestion to keep query performance and storage costs under control.

Usage: spark-submit spark_jobs/maintenance.py
       (or: make maintenance)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

from spark_jobs.utils.spark_session import build_spark_session

NS    = os.environ["POLARIS_NAMESPACE"]
TABLE = f"polaris.{NS}.sales"   # used in CALL statements
# Iceberg stored procedures need the catalog-qualified name
QUALIFIED = f"{NS}.sales"       # catalog is implied by 'CALL polaris.system.*'

# Target file size: 128 MiB is the Iceberg default sweet spot
TARGET_FILE_SIZE = str(128 * 1024 * 1024)
# Keep snapshots for 7 days; always retain at least the last 5
SNAPSHOT_RETENTION_DAYS = 7
SNAPSHOT_MIN_KEEP       = 5
# Orphan files older than 3 days are safe to delete (3d > any reasonable job duration)
ORPHAN_MIN_AGE_DAYS = 3


def section(title: str) -> None:
    print(f"\n{'═' * 60}")
    print(f"  {title}")
    print('═' * 60)


def show_stats(spark, label: str) -> None:
    section(f"Table stats — {label}")
    spark.sql(f"""
        SELECT
            content,
            COUNT(*)               AS file_count,
            SUM(record_count)      AS total_records,
            SUM(file_size_in_bytes) / 1024 / 1024 AS total_size_mb
        FROM   {TABLE}.files
        GROUP BY content
    """).show()
    spark.sql(f"SELECT COUNT(*) AS snapshot_count FROM {TABLE}.snapshots").show()


def compact_data_files(spark) -> None:
    section("Step 1 — Compact data files (bin-pack)")
    result = spark.sql(f"""
        CALL polaris.system.rewrite_data_files(
            table    => '{QUALIFIED}',
            strategy => 'binpack',
            options  => map(
                'target-file-size-bytes', '{TARGET_FILE_SIZE}',
                'min-input-files',        '2',
                'max-concurrent-file-group-rewrites', '5'
            )
        )
    """)
    result.show()


def rewrite_manifests(spark) -> None:
    section("Step 2 — Rewrite manifest files")
    result = spark.sql(f"""
        CALL polaris.system.rewrite_manifests(
            table                => '{QUALIFIED}',
            use_caching          => true
        )
    """)
    result.show()


def expire_snapshots(spark) -> None:
    section(f"Step 3 — Expire snapshots older than {SNAPSHOT_RETENTION_DAYS} days")
    cutoff = (datetime.now(timezone.utc) - timedelta(days=SNAPSHOT_RETENTION_DAYS)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    result = spark.sql(f"""
        CALL polaris.system.expire_snapshots(
            table       => '{QUALIFIED}',
            older_than  => TIMESTAMP '{cutoff}',
            retain_last => {SNAPSHOT_MIN_KEEP}
        )
    """)
    result.show()


def remove_orphan_files(spark) -> None:
    section(f"Step 4 — Remove orphan files older than {ORPHAN_MIN_AGE_DAYS} days")
    cutoff = (datetime.now(timezone.utc) - timedelta(days=ORPHAN_MIN_AGE_DAYS)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    result = spark.sql(f"""
        CALL polaris.system.remove_orphan_files(
            table      => '{QUALIFIED}',
            older_than => TIMESTAMP '{cutoff}'
        )
    """)
    result.show()


def main() -> None:
    spark = build_spark_session("iceberg-maintenance")

    show_stats(spark, "before maintenance")

    compact_data_files(spark)
    rewrite_manifests(spark)
    expire_snapshots(spark)
    remove_orphan_files(spark)

    show_stats(spark, "after maintenance")

    spark.stop()


if __name__ == "__main__":
    main()
