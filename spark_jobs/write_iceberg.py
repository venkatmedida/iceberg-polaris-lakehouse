#!/usr/bin/env python3
"""
Write sample sales data to an Iceberg table in ADLS via Polaris.

Table reference in Spark: polaris.<namespace>.<table>
  - 'polaris'     = Spark catalog alias (points to Polaris REST endpoint)
  - <namespace>   = Polaris namespace created in 01_setup_polaris.py
  - <table>       = Iceberg table (created here if it doesn't exist)

Usage: spark-submit spark_jobs/write_iceberg.py
       (or: make write)
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date
from dotenv import load_dotenv
from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DateType, DoubleType,
)

load_dotenv()

from spark_jobs.utils.spark_session import build_spark_session

NS    = os.environ["POLARIS_NAMESPACE"]
TABLE = f"polaris.{NS}.sales"


def create_table(spark) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS polaris.{NS}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            order_id    STRING  NOT NULL,
            customer_id STRING  NOT NULL,
            product     STRING,
            quantity    INT,
            amount      DOUBLE,
            order_date  DATE    NOT NULL
        )
        USING iceberg
        PARTITIONED BY (order_date)
        TBLPROPERTIES (
            'write.format.default'              = 'parquet',
            'write.parquet.compression-codec'   = 'snappy',
            'write.metadata.compression-codec'  = 'gzip',
            'write.metadata.metrics.default'    = 'full',
            'history.expire.max-snapshot-age-ms'= '604800000'
        )
    """)


def write_batch(spark, rows: list, label: str) -> None:
    schema = StructType([
        StructField("order_id",    StringType(),  False),
        StructField("customer_id", StringType(),  False),
        StructField("product",     StringType(),  True),
        StructField("quantity",    IntegerType(), True),
        StructField("amount",      DoubleType(),  True),
        StructField("order_date",  DateType(),    False),
    ])
    df = spark.createDataFrame(rows, schema)
    print(f"Writing {label}: {df.count()} rows → {TABLE}")
    df.writeTo(TABLE).append()


def main() -> None:
    spark = build_spark_session("iceberg-write-demo")

    create_table(spark)

    # First batch — simulates an initial load
    write_batch(spark, [
        Row("ORD-001", "CUST-A", "Widget Pro",   2,  49.99, date(2024, 1, 15)),
        Row("ORD-002", "CUST-B", "Gadget Plus",  1, 129.99, date(2024, 1, 15)),
        Row("ORD-003", "CUST-A", "Widget Pro",   5, 124.95, date(2024, 1, 16)),
        Row("ORD-004", "CUST-C", "Super Tool",   3,  89.97, date(2024, 1, 17)),
    ], "batch-1")

    # Second batch — a new snapshot is created; old snapshot survives for time travel
    write_batch(spark, [
        Row("ORD-005", "CUST-D", "Widget Pro",  10, 249.90, date(2024, 1, 18)),
        Row("ORD-006", "CUST-A", "Gadget Plus",  2, 259.98, date(2024, 1, 18)),
    ], "batch-2")

    print(f"\nTotal rows : {spark.table(TABLE).count()}")

    print("\nSnapshot history:")
    spark.sql(f"SELECT snapshot_id, parent_id, made_current_at, is_current_ancestor FROM {TABLE}.history").show(truncate=False)

    print("\nPartitions:")
    spark.sql(f"SELECT partition, record_count, file_count FROM {TABLE}.partitions").show()

    spark.stop()


if __name__ == "__main__":
    main()
