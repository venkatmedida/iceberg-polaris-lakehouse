# Unlocking Snowflake for AI Analytics with Open Iceberg and Apache Polaris

*How organizations can extend Snowflake's analytical power across any compute engine — without duplicating data*

**Full source code: [github.com/venkatmedida/iceberg-polaris-lakehouse](https://github.com/venkatmedida/iceberg-polaris-lakehouse)**

---

## The Enterprise Challenge

Snowflake is where organizations run their most critical analytical workloads — dashboards, revenue reporting, customer 360, and increasingly, AI feature stores and ML pipelines. But modern data platforms rarely live inside a single engine. Spark pipelines ingest and transform raw data at scale; Python notebooks run feature engineering; specialized ML runtimes train models. Each of these produces data that Snowflake analysts and AI applications need to consume — and traditionally that meant one of two painful options: duplicate the data into Snowflake, or maintain fragile ETL pipelines that lag behind.

Apache Iceberg changes the storage model. What this post adds is the missing piece: **a self-hosted [Apache Polaris](https://polaris.apache.org/) instance as the neutral Iceberg REST catalog**, so that Snowflake can read and write the same table that Spark is writing to — in real time, from the same parquet files, without any data movement. Snowflake becomes a first-class citizen of an open lakehouse where every engine's commit is immediately visible to every other.

For organizations already invested in Snowflake for AI analytics, this architecture means your feature pipelines, ML training jobs, and data science workflows running on Spark can feed data directly into Snowflake — as native Iceberg snapshots that Snowflake queries as if they were its own managed tables.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Apache Polaris 1.5.0                     │
│                    (Iceberg REST Catalog — Docker)              │
│                                                                 │
│   • Single source of truth for table metadata + snapshots       │
│   • OAuth2 client credentials (PRINCIPAL_ROLE:ALL scope)        │
│   • Azure credential vending (rawdl-scoped SAS tokens)          │
└────────────────────┬──────────────────────┬─────────────────────┘
                     │  REST + OAuth2        │  REST + OAuth2
                     │                       │  + VENDED_CREDENTIALS
          ┌──────────┴──────────┐   ┌────────┴──────────────────────┐
          │     PySpark 3.5     │   │     Snowflake (Horizon)       │
          │                     │   │                               │
          │  Feature pipelines  │   │  AI Analytics / BI / SQL      │
          │  ML training data   │   │  CATALOG INTEGRATION          │
          │  Data ingestion      │   │    VENDED_CREDENTIALS         │
          │  Table maintenance  │   │  LINKED_CATALOG               │
          │                     │   │  (auto-discovers all tables   │
          │                     │   │   written by any engine)      │
          └──────────┬──────────┘   └────────┬──────────────────────┘
                     │                        │
                     └───────────┬────────────┘
                                 │ abfss://
                    ┌────────────┴────────────┐
                    │   Azure ADLS Gen2        │
                    │   Shared Parquet files   │
                    │   + Iceberg metadata     │
                    └─────────────────────────┘
```

Every engine — Spark, Snowflake, or any other Iceberg-compatible runtime — reads and writes directly to ADLS. Polaris is the single catalog that tracks what exists and where. There is no copy, no sync job, no pipeline between engines.

---

## Stack

| Component | Version | Role |
|-----------|---------|------|
| Apache Polaris | 1.5.0 | Iceberg REST catalog (Docker) |
| PySpark | 3.5.3 | Data ingestion, feature prep, maintenance |
| `iceberg-spark-runtime` | 1.7.1 | Iceberg extensions for Spark |
| `hadoop-azure` | 3.3.4 | ABFS driver for ADLS Gen2 |
| Azure ADLS Gen2 | — | Shared parquet + metadata storage |
| Snowflake | — | AI analytics, BI, SQL read/write via Horizon catalog |

---

## Part 1 — Standing Up Polaris

Polaris runs as a Docker container with minimal configuration. The key bootstrap detail for Polaris 1.5.0 is that the default realm is `POLARIS` (uppercase):

```yaml
# docker-compose.yml (excerpt)
environment:
  POLARIS_BOOTSTRAP_CREDENTIALS: "POLARIS,${POLARIS_ROOT_CLIENT_ID},${POLARIS_ROOT_CLIENT_SECRET}"
  AZURE_CLIENT_ID: ${AZURE_POLARIS_CLIENT_ID}
  AZURE_CLIENT_SECRET: ${AZURE_POLARIS_CLIENT_SECRET}
  AZURE_TENANT_ID: ${AZURE_TENANT_ID}
```

The Azure service principal on the Polaris container is what enables **credential vending** — Polaris uses it to mint short-lived scoped SAS tokens for every client that requests table access. Clients — including Snowflake — never hold long-lived storage credentials.

[`setup/01_setup_polaris.py`](https://github.com/venkatmedida/iceberg-polaris-lakehouse/blob/main/setup/01_setup_polaris.py) bootstraps the catalog via the Polaris Management API: creates the catalog, namespace, and a dedicated service principal for client access.

```bash
make up      # start Polaris + Postgres
make setup   # bootstrap catalog, namespace, service principal
```

---

## Part 2 — Spark Writes Data That Snowflake Will Analyze

Spark handles ingestion and feature engineering — the classic data engineering role. Each write creates a new Iceberg snapshot in Polaris that Snowflake can immediately query.

### SparkSession Configuration

```python
# spark_jobs/utils/spark_session.py (key configs)
.config("spark.sql.catalog.polaris",           "org.apache.iceberg.spark.SparkCatalog")
.config("spark.sql.catalog.polaris.type",      "rest")
.config("spark.sql.catalog.polaris.uri",       POLARIS_URI)
.config("spark.sql.catalog.polaris.credential",f"{POLARIS_CLIENT_ID}:{POLARIS_CLIENT_SECRET}")
.config("spark.sql.catalog.polaris.scope",     "PRINCIPAL_ROLE:ALL")
.config("spark.sql.catalog.polaris.warehouse", POLARIS_CATALOG_NAME)
.config("spark.sql.catalog.polaris.oauth2-server-uri",
        f"{POLARIS_URI}/v1/oauth/tokens")
# HadoopFileIO: avoids ADLSFileIO dependency on Azure SDK v12
.config("spark.sql.catalog.polaris.io-impl",
        "org.apache.iceberg.hadoop.HadoopFileIO")
# Snowflake writes parquet with DELTA_LENGTH_BYTE_ARRAY encoding;
# Iceberg's vectorized reader can't handle it — disable for cross-engine reads
.config("spark.sql.iceberg.vectorization.enabled", "false")
```

The vectorized reader config is critical for bidirectional use: Snowflake writes parquet files with `DELTA_LENGTH_BYTE_ARRAY` encoding for string columns, which Iceberg's own vectorized reader doesn't support. Disabling it allows Spark to read Snowflake-written files transparently.

### Writing Feature Data

```python
# spark_jobs/write_iceberg.py
df.writeTo("polaris.demo.sales").append()
```

Each append creates a new Iceberg snapshot. The moment the snapshot is committed to Polaris, it's queryable from Snowflake — no pipeline, no delay.

```bash
make write
```

### Time Travel for Reproducible ML

Iceberg's time travel is natively available through Polaris:

```python
# spark_jobs/read_iceberg.py
first_snap = spark.sql(f"SELECT snapshot_id FROM {TABLE}.history ORDER BY made_current_at").first()
spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF {first_snap['snapshot_id']}").show()
```

ML pipelines that need to reproduce a training dataset from a specific point in time can reference a snapshot ID — exact reproducibility regardless of subsequent writes by any engine.

```bash
make read
```

---

## Part 3 — Iceberg Table Maintenance

Polaris manages metadata; table maintenance (compaction, manifest rewriting, snapshot expiry) runs through Spark stored procedures:

```python
# spark_jobs/maintenance.py

# Compact small files — critical for Snowflake query performance
spark.sql(f"""
    CALL polaris.system.rewrite_data_files(
        table    => 'demo.sales',
        strategy => 'binpack',
        options  => map('target-file-size-bytes', '134217728')
    )
""")

# Consolidate manifests after many incremental writes
spark.sql("CALL polaris.system.rewrite_manifests(table => 'demo.sales', use_caching => true)")

# Expire old snapshots — keep last 5, drop older than 7 days
cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
spark.sql(f"""
    CALL polaris.system.expire_snapshots(
        table       => 'demo.sales',
        older_than  => TIMESTAMP '{cutoff}',
        retain_last => 5
    )
""")

# Remove orphan files from aborted writes
spark.sql(f"""
    CALL polaris.system.remove_orphan_files(
        table      => 'demo.sales',
        older_than => TIMESTAMP '{cutoff}'
    )
""")
```

Well-compacted files and clean manifests directly improve Snowflake's scan performance on the same table — maintenance done in Spark benefits Snowflake queries immediately.

> **Gotcha**: Spark's CALL procedure parser only accepts literal values in named arguments — no function calls with parentheses. Compute timestamps in Python and embed them as `TIMESTAMP 'yyyy-MM-dd HH:mm:ss'` literals.

```bash
make maintenance
```

---

## Part 4 — Snowflake as the AI Analytics Engine

This is where the architecture pays off for organizations using Snowflake for AI and analytics workloads. Snowflake connects to Polaris through a **Catalog Integration** with `VENDED_CREDENTIALS` — no data movement, no staging, no ETL.

### How Credential Vending Works

```
┌──────────────┐   1. OAuth token request      ┌──────────────────┐
│  Snowflake   │ ──────────────────────────────▶│   Polaris        │
│              │◀────────────────────────────── │   /oauth/tokens  │
│              │   JWT access token             └──────────────────┘
│              │
│              │   2. GET /tables/sales          ┌──────────────────┐
│              │   + X-Iceberg-Access-Delegation │   Polaris        │
│              │ ──────────────────────────────▶ │   /namespaces/   │
│              │                                 │   demo/tables/   │
│              │◀────────────────────────────── │   sales          │
│              │   table metadata                └──────────────────┘
│              │   + adls.sas-token.*
│              │     (rawdl scope, 1h TTL)
│              │
│              │   3. Read/write parquet   ┌──────────────────────┐
│              │ ────────────────────────▶ │  Azure ADLS Gen2     │
└──────────────┘   directly via SAS token  └──────────────────────┘
```

Snowflake sends `X-Iceberg-Access-Delegation: vended-credentials` on every table request. Polaris responds with table metadata plus a short-lived Azure SAS token with `sp=rawdl` permissions — read, add, **write**, delete, and list. **No external volume is required.** The Azure service principal lives only inside Polaris; Snowflake never holds a long-lived storage credential.

### Catalog Integration

```sql
-- setup/02b_snowflake_catalog_linked_db.sql
CREATE OR REPLACE CATALOG INTEGRATION polaris_catalog_int
  CATALOG_SOURCE    = ICEBERG_REST
  TABLE_FORMAT      = ICEBERG
  CATALOG_NAMESPACE = 'demo'
  REST_CONFIG = (
    CATALOG_URI            = 'https://<POLARIS_PUBLIC_HOST>/api/catalog'
    WAREHOUSE              = '<POLARIS_CATALOG_NAME>'
    ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
  )
  REST_AUTHENTICATION = (
    TYPE                 = OAUTH
    OAUTH_CLIENT_ID      = '<POLARIS_CLIENT_ID>'
    OAUTH_CLIENT_SECRET  = '<POLARIS_CLIENT_SECRET>'
    OAUTH_ALLOWED_SCOPES = ('PRINCIPAL_ROLE:ALL')
  )
  ENABLED = TRUE;
```

Snowflake auto-resolves the OAuth token endpoint from the catalog's `GET /config` response — no need to specify it explicitly.

### Linked Database — Snowflake Auto-Discovers Everything

The `LINKED_CATALOG` syntax triggers a background sync: Snowflake calls Polaris's namespace and table listing APIs and surfaces every namespace as a Snowflake schema — automatically, without manual `CREATE ICEBERG TABLE` statements:

```sql
-- schemas and tables from Polaris appear in Snowflake automatically
CREATE OR REPLACE DATABASE polaris_linked_db
  LINKED_CATALOG = (
    CATALOG            = 'polaris_catalog_int'
    BLOCKED_NAMESPACES = ('information_schema')
  );

-- monitor sync (typically completes in ~30s)
SELECT SYSTEM$CATALOG_LINK_STATUS('polaris_linked_db');
-- {"executionState":"RUNNING"} → in progress
-- {"executionState":"DONE"}    → demo schema and sales table are live
```

Once the sync completes, Snowflake analysts can immediately run SQL on data written by Spark, Python, or any other Iceberg-compatible engine — with no awareness that the data originated outside Snowflake.

```bash
make sf-setup
```

---

## Part 5 — Snowflake Reads, Writes, and Updates

The [`setup/03_snowflake_read_write_test.sql`](https://github.com/venkatmedida/iceberg-polaris-lakehouse/blob/main/setup/03_snowflake_read_write_test.sql) script validates every direction of the interoperability:

### Reading Spark-Written Data

```sql
-- Snowflake immediately sees all rows written by Spark
SELECT * FROM polaris_linked_db.demo.sales ORDER BY order_date, order_id;
-- 6 rows (ORD-001 … ORD-006) ✓

SELECT
    order_date,
    COUNT(*)    AS orders,
    SUM(amount) AS total_revenue
FROM polaris_linked_db.demo.sales
GROUP BY order_date
ORDER BY order_date;
```

For AI analytics use cases, this means Snowflake can run feature queries, aggregations, or Cortex AI functions on data that a Spark feature pipeline just committed — in the same transaction boundary, no delay.

### Snowflake Writing Back

```sql
-- Snowflake inserts create new Iceberg snapshots in Polaris
INSERT INTO polaris_linked_db.demo.sales
VALUES ('ORD-SF-001', 'CUST-SF', 'Cloud Widget', 5, 149.95, CURRENT_DATE());

INSERT INTO polaris_linked_db.demo.sales VALUES
    ('ORD-SF-002', 'CUST-A', 'Cloud Widget Pro',  3, 299.85, CURRENT_DATE()),
    ('ORD-SF-003', 'CUST-B', 'Snowflake Gadget',  1,  89.99, CURRENT_DATE());

-- 9 rows total (6 Spark + 3 Snowflake)
SELECT COUNT(*) AS total_rows FROM polaris_linked_db.demo.sales;
```

Snowflake-written rows are visible to Spark immediately after the INSERT commits. This enables write-back patterns for AI workloads: Snowflake Cortex computes predictions or scores and writes them back to the Iceberg table, where Spark pipelines pick them up for the next training cycle.

### Cross-Engine MERGE

```sql
-- Snowflake updates a row originally written by Spark
MERGE INTO polaris_linked_db.demo.sales AS target
USING (
    SELECT 'ORD-001' AS order_id, 'Widget Pro' AS product,
           10 AS quantity, 249.90 AS amount
) AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET quantity = source.quantity, amount = source.amount
WHEN NOT MATCHED THEN INSERT (...) VALUES (...);
-- 0 inserted, 1 updated ✓
```

```bash
make sf-test
```

### Spark Confirms It Sees Everything

After the Snowflake script runs, `make read` from Spark shows the complete picture:

```
+----------+-----------+----------------+--------+------+----------+
|  order_id|customer_id|         product|quantity|amount|order_date|
+----------+-----------+----------------+--------+------+----------+
|   ORD-001|     CUST-A|      Widget Pro|      10| 249.9|2024-01-15|  ← updated by Snowflake MERGE
|   ORD-002|     CUST-B|     Gadget Plus|       1|129.99|2024-01-15|
...
|ORD-SF-001|    CUST-SF|    Cloud Widget|       5|149.95|2026-07-03|  ← written by Snowflake
|ORD-SF-002|     CUST-A|Cloud Widget Pro|       3|299.85|2026-07-03|  ← written by Snowflake
|ORD-SF-003|     CUST-B|Snowflake Gadget|       1| 89.99|2026-07-03|  ← written by Snowflake
+----------+-----------+----------------+--------+------+----------+
```

The snapshot history shows 8 entries — each engine's commit is a first-class Iceberg snapshot. Spark can time-travel to any Snowflake snapshot; Snowflake can reference any Spark snapshot.

```bash
make read    # Spark confirms all 9 rows including Snowflake's commits
```

---

## What This Means for AI Analytics on Snowflake

The architectural pattern demonstrated here has direct implications for organizations building AI pipelines with Snowflake at the center:

**Feature stores without data duplication.** Feature engineering pipelines running on Spark write computed features directly to an Iceberg table in Polaris. Snowflake Cortex or ML models running inside Snowflake query those features from the same table — no copy, no sync, no staleness.

**Model inference write-back.** Snowflake Cortex generates predictions and writes them as new rows or updates to an Iceberg table in Polaris. Downstream Spark jobs pick up those predictions for retraining, evaluation, or further pipeline stages — from the same table, with full snapshot history.

**Unified governance.** Because Polaris is the single catalog, access control, schema evolution, and table lifecycle are managed in one place. Snowflake's RBAC and Polaris's principal role model can be layered to enforce consistent data governance regardless of which engine writes the data.

**Cost-optimized compute.** Heavy transformation and ingestion runs on Spark (cheaper for large-scale processing); interactive analytics, dashboards, and AI inference run on Snowflake (optimized for concurrency and SQL). Neither engine needs to know about the other's internal mechanics — they both speak Iceberg through Polaris.

---

## Key Learnings


**1. Credential vending supports writes — no external volume needed**
With `ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS`, Polaris mints SAS tokens with `sp=rawdl` scope (read + add + write + delete + list). Snowflake reads the table's base location from Polaris metadata and writes parquet files directly to ADLS. `SHOW ICEBERG TABLES` confirms: `external_volume_name: <invalid>`, `can_write_metadata: Y`.

**2. Disable Iceberg's vectorized reader for cross-engine tables**
Snowflake writes parquet with `DELTA_LENGTH_BYTE_ARRAY` encoding. Iceberg's own vectorized reader (`spark.sql.iceberg.vectorization.enabled=false`) doesn't support it — disabling it (distinct from `spark.sql.parquet.enableVectorizedReader`) allows Spark to read Snowflake-written files without errors.

**3. Spark CALL procedures require literal timestamps**
Named procedure arguments in Spark's CALL parser don't accept function calls with parentheses. Compute timestamps in Python and embed as `TIMESTAMP 'yyyy-MM-dd HH:mm:ss'` literals.

**4. Compaction in Spark improves Snowflake query performance**
Running `rewrite_data_files` in Spark (targeting 128 MB files) reduces the number of parquet files Snowflake must scan per query — maintenance done in one engine benefits all engines.

**6. Polaris realm name in 1.5.0**
The default realm is `POLARIS` (uppercase), not `default-realm`. Using the wrong realm causes silent `unauthorized_client` errors. Every Polaris log line includes the realm in brackets: `[requestId,POLARIS]`.

---

## Running It Yourself

```bash
git clone https://github.com/venkatmedida/iceberg-polaris-lakehouse
cd iceberg-polaris-lakehouse
cp .env.example .env   # fill in your Azure + Polaris credentials

make up          # start Polaris (Docker)
make setup       # bootstrap catalog + service principal
make write       # Spark writes feature data (6 rows, 2 snapshots)
make read        # Spark reads with time travel + metadata tables
make maintenance # compact files, rewrite manifests, expire snapshots

# Connect Snowflake (requires snow CLI + ngrok for local Polaris)
ngrok http 8181              # expose Polaris publicly → set POLARIS_PUBLIC_HOST
make sf-setup                # create catalog integration + linked database
make sf-test                 # bidirectional read/write/merge test
make read                    # Spark confirms it sees all Snowflake snapshots
```

Full source: **[github.com/venkatmedida/iceberg-polaris-lakehouse](https://github.com/venkatmedida/iceberg-polaris-lakehouse)**
