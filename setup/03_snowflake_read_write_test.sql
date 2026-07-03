-- ================================================================
-- Bidirectional interoperability test: Spark ↔ Polaris ↔ Snowflake
--
-- Demonstrates that the same Iceberg table in Polaris is readable
-- and writable from both Spark and Snowflake, with each engine
-- seeing the other's commits as new Iceberg snapshots.
--
-- Table used throughout: polaris_linked_db.demo.sales
--   Backed by: polaris_catalog_int (VENDED_CREDENTIALS)
--   Linked via: LINKED_CATALOG — schemas/tables auto-discovered
--   Storage: abfss://<AZURE_CONTAINER>@<AZURE_STORAGE_ACCOUNT>.dfs.core.windows.net
--
-- Run order:
--   1. Ensure Spark write job has already run (6 rows baseline)
--   2. Run this script in Snowflake
--   3. Run: spark-submit ... spark_jobs/read_iceberg.py   to verify from Spark
-- ================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE polaris_linked_db;
USE SCHEMA demo;

-- ════════════════════════════════════════════════════════════════
--  Phase 1 — Snowflake reads what Spark wrote
-- ════════════════════════════════════════════════════════════════

SELECT '── Phase 1: Snowflake reads Spark-written data ──' AS phase;

-- Full table — expect 6 rows (ORD-001 … ORD-006) written by Spark
SELECT * FROM sales ORDER BY order_date, order_id;

-- Aggregation matches Spark output
SELECT
    order_date,
    COUNT(*)    AS orders,
    SUM(amount) AS total_revenue
FROM sales
GROUP BY order_date
ORDER BY order_date;

-- Snapshot metadata — should show 3 snapshots (2 Spark batches + 1 manifest rewrite)
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('polaris_linked_db.demo.sales');

-- Baseline row count (must be 6 before Snowflake writes)
SELECT COUNT(*) AS rows_before_sf_write FROM sales;

-- ════════════════════════════════════════════════════════════════
--  Phase 2 — Snowflake writes new rows (new Iceberg snapshots)
-- ════════════════════════════════════════════════════════════════
-- The linked catalog uses VENDED_CREDENTIALS: Polaris mints a
-- short-lived Azure SAS token (rawdl scope = read+add+write+delete+list)
-- per commit. Snowflake gets the table's base location from Polaris
-- metadata, writes parquet files to ADLS using the SAS token, then
-- registers the new snapshot in Polaris. No external volume needed.

SELECT '── Phase 2: Snowflake inserts new rows ──' AS phase;

-- Single-row INSERT from Snowflake
INSERT INTO sales (order_id, customer_id, product, quantity, amount, order_date)
VALUES ('ORD-SF-001', 'CUST-SF', 'Cloud Widget', 5, 149.95, CURRENT_DATE());

-- Batch INSERT (multiple rows, single snapshot)
INSERT INTO sales (order_id, customer_id, product, quantity, amount, order_date)
VALUES
    ('ORD-SF-002', 'CUST-A', 'Cloud Widget Pro',  3, 299.85, CURRENT_DATE()),
    ('ORD-SF-003', 'CUST-B', 'Snowflake Gadget',  1,  89.99, CURRENT_DATE());

-- ════════════════════════════════════════════════════════════════
--  Phase 3 — Snowflake reads back its own writes
-- ════════════════════════════════════════════════════════════════

SELECT '── Phase 3: Snowflake reads after its own writes ──' AS phase;

-- Expect 9 rows total (6 Spark + 3 Snowflake)
SELECT COUNT(*) AS total_rows FROM sales;

-- Snowflake-written rows only
SELECT * FROM sales
WHERE order_id LIKE 'ORD-SF-%'
ORDER BY order_id;

-- Updated aggregation including today's Snowflake orders
SELECT
    order_date,
    COUNT(*)    AS orders,
    SUM(amount) AS total_revenue
FROM sales
GROUP BY order_date
ORDER BY order_date;

-- ════════════════════════════════════════════════════════════════
--  Phase 4 — Time travel: read table at Spark baseline state
-- ════════════════════════════════════════════════════════════════
-- Each Snowflake INSERT creates a new Iceberg snapshot. We can use
-- Iceberg time travel to read the table before Snowflake wrote to it.

SELECT '── Phase 4: Time travel to Spark-only state ──' AS phase;

-- Get the oldest snapshot ID (the first Spark batch write)
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('polaris_linked_db.demo.sales');
-- Use the snapshot_id from the output above:
-- SELECT * FROM sales AT (VERSION => <first_snapshot_id>);

-- ════════════════════════════════════════════════════════════════
--  Phase 5 — MERGE upsert (Snowflake updates a Spark-written row)
-- ════════════════════════════════════════════════════════════════

SELECT '── Phase 5: MERGE upsert across engines ──' AS phase;

-- Upsert: increase quantity for ORD-001 (originally written by Spark)
-- and insert a new row if it doesn't exist
MERGE INTO sales AS target
USING (
    SELECT 'ORD-001'   AS order_id,
           'CUST-A'    AS customer_id,
           'Widget Pro' AS product,
           10           AS quantity,   -- was 2, now updated to 10
           249.90       AS amount,     -- was 49.99, now updated
           DATE '2024-01-15' AS order_date
) AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET
    quantity   = source.quantity,
    amount     = source.amount
WHEN NOT MATCHED THEN INSERT
    (order_id, customer_id, product, quantity, amount, order_date)
    VALUES (source.order_id, source.customer_id, source.product,
            source.quantity, source.amount, source.order_date);

-- Confirm ORD-001 reflects Snowflake's update
SELECT * FROM sales WHERE order_id = 'ORD-001';

-- ════════════════════════════════════════════════════════════════
--  Phase 6 — Snapshot audit: every write made a new snapshot
-- ════════════════════════════════════════════════════════════════

SELECT '── Phase 6: Snapshot history ──' AS phase;

-- Each INSERT and MERGE above is a separate Iceberg snapshot in Polaris.
-- Spark will see all of them when it reads the table next.
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('polaris_linked_db.demo.sales');

-- ════════════════════════════════════════════════════════════════
--  Phase 7 — Spark verification (run from terminal after this script)
-- ════════════════════════════════════════════════════════════════
--
-- cd <project-root>
--
-- spark-submit \
--   --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,org.apache.hadoop:hadoop-azure:3.3.4 \
--   --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--   spark_jobs/read_iceberg.py
--
-- Expected Spark output:
--   Total rows  : 9  (6 Spark originals + 3 Snowflake inserts)
--   ORD-001     : quantity=10, amount=249.90  (updated via Snowflake MERGE)
--   Snapshot history: multiple snapshots showing both Spark and Snowflake commits
