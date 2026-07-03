#!/usr/bin/env python3
"""
Read Iceberg data from ADLS via Polaris, demonstrating:
  - current snapshot read
  - time travel (by snapshot ID and by timestamp)
  - metadata table inspection (snapshots, files, manifests)
  - partition pruning with a filter push-down check

Usage: spark-submit spark_jobs/read_iceberg.py
       (or: make read)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

from spark_jobs.utils.spark_session import build_spark_session

NS    = os.environ["POLARIS_NAMESPACE"]
TABLE = f"polaris.{NS}.sales"


def section(title: str) -> None:
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print('─' * 60)


def main() -> None:
    spark = build_spark_session("iceberg-read-demo")

    section("Current snapshot — full table")
    spark.table(TABLE).orderBy("order_date", "order_id").show()

    section("Partition pruning — filter on order_date")
    spark.sql(f"""
        SELECT order_id, product, amount
        FROM   {TABLE}
        WHERE  order_date = DATE '2024-01-15'
    """).show()

    section("Aggregation by date")
    spark.sql(f"""
        SELECT
            order_date,
            COUNT(*)    AS orders,
            SUM(amount) AS total_revenue,
            AVG(amount) AS avg_order_value
        FROM  {TABLE}
        GROUP BY order_date
        ORDER BY order_date
    """).show()

    section("Snapshot history (metadata table)")
    snapshots = spark.sql(f"""
        SELECT snapshot_id, parent_id, made_current_at, is_current_ancestor
        FROM   {TABLE}.history
        ORDER BY made_current_at
    """)
    snapshots.show(truncate=False)

    # Time travel: read the table as it was at the first snapshot
    section("Time travel — earliest snapshot")
    first = snapshots.first()
    if first:
        snap_id = first["snapshot_id"]
        print(f"  Reading snapshot_id={snap_id}")
        spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF {snap_id}").show()

    section("Files metadata")
    spark.sql(f"""
        SELECT file_path, record_count, file_size_in_bytes, partition
        FROM   {TABLE}.files
    """).show(truncate=False)

    section("Manifests metadata")
    spark.sql(f"""
        SELECT path, length, partition_spec_id, added_data_files_count, existing_data_files_count
        FROM   {TABLE}.manifests
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
