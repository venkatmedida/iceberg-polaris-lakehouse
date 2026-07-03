# Building a Bidirectional Iceberg Lakehouse with Apache Polaris, Spark, and Snowflake

*Open-source catalog, two compute engines, one table — zero credential sprawl*

---

## The Problem

Modern data platforms need to serve multiple engines from the same data. A Spark pipeline writes raw events; a Snowflake analyst queries aggregations; another Spark job runs maintenance. Traditionally this required either data duplication or brittle ETL pipelines between systems.

Apache Iceberg solves the storage side. What's been missing is a neutral, open catalog that any engine can talk to — so that a row written by Spark is immediately visible to Snowflake without a copy.

This post walks through exactly that: a self-hosted [Apache Polaris](https://polaris.apache.org/) instance acting as the Iceberg REST catalog, with PySpark and Snowflake both reading and writing the same table, seeing each other's commits as native Iceberg snapshots.

All code is at: **https://github.com/venkatmedida/iceberg-polaris-lakehouse**

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Apache Polaris 1.5.0                     │
│                    (Iceberg REST Catalog — Docker)              │
│                                                                 │
│   • Namespace + table registry                                  │
│   • OAuth2 client credentials (PRINCIPAL_ROLE:ALL scope)        │
│   • Azure credential vending (rawdl-scoped SAS tokens)          │
└────────────────────┬──────────────────────┬─────────────────────┘
                     │  REST + OAuth2        │  REST + OAuth2
                     │                       │  + X-Iceberg-Access-
                     │                       │    Delegation: vended-credentials
          ┌──────────┴──────────┐   ┌────────┴──────────────────────┐
          │     PySpark 3.5     │   │        Snowflake              │
          │                     │   │                               │
          │  • write_iceberg.py │   │  CATALOG INTEGRATION          │
          │  • read_iceberg.py  │   │    VENDED_CREDENTIALS         │
          │  • maintenance.py   │   │  LINKED_CATALOG               │
          │  HadoopFileIO       │   │    (auto-discovers schemas     │
          │  ClientCreds OAuth  │   │     and tables from Polaris)  │
          └──────────┬──────────┘   └────────┬──────────────────────┘
                     │                        │
                     └───────────┬────────────┘
                                 │ abfss://
                    ┌────────────┴────────────┐
                    │   Azure ADLS Gen2        │
                    │   (Parquet + Iceberg     │
                    │    metadata files)       │
                    └─────────────────────────┘
```

---

## Stack

| Component | Version | Role |
|-----------|---------|------|
| Apache Polaris | 1.5.0 | Iceberg REST catalog (Docker) |
| PySpark | 3.5.3 | Write, read, maintenance |
| `iceberg-spark-runtime` | 1.7.1 | Iceberg extensions for Spark |
| `hadoop-azure` | 3.3.4 | ABFS driver for ADLS Gen2 |
| Azure ADLS Gen2 | — | Parquet + metadata storage |
| Snowflake | — | SQL read/write via Horizon catalog |

---

## Part 1 — Standing Up Polaris

Polaris runs as a Docker container. The key bootstrap configuration is the realm name — in Polaris 1.5.0 the default realm is `POLARIS` (uppercase), not `default-realm`:

```yaml
# docker-compose.yml (excerpt)
environment:
  POLARIS_BOOTSTRAP_CREDENTIALS: "POLARIS,${POLARIS_ROOT_CLIENT_ID},${POLARIS_ROOT_CLIENT_SECRET}"
  AZURE_CLIENT_ID: ${AZURE_POLARIS_CLIENT_ID}
  AZURE_CLIENT_SECRET: ${AZURE_POLARIS_CLIENT_SECRET}
  AZURE_TENANT_ID: ${AZURE_TENANT_ID}
```

The Azure service principal credentials on the Polaris container are what enable **credential vending** — Polaris uses them to mint short-lived SAS tokens on behalf of clients.

[`setup/01_setup_polaris.py`](https://github.com/venkatmedida/iceberg-polaris-lakehouse/blob/main/setup/01_setup_polaris.py) bootstraps the catalog via the Polaris Management API: creates the catalog, namespace, and a service principal, then writes the generated `POLARIS_CLIENT_ID` and `POLARIS_CLIENT_SECRET` back to `.env`.

```bash
make up      # starts Polaris + Postgres
make setup   # bootstraps catalog, namespace, principal
```

---

## Part 2 — Writing Iceberg Data with Spark

### SparkSession Configuration

The session wires up the Polaris REST catalog as a named Spark catalog (`polaris`):

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
# Force HadoopFileIO — avoids loading ADLSFileIO which requires Azure SDK v12
.config("spark.sql.catalog.polaris.io-impl",
        "org.apache.iceberg.hadoop.HadoopFileIO")
# Snowflake writes parquet with DELTA_LENGTH_BYTE_ARRAY encoding;
# Iceberg's vectorized reader can't handle it — disable for cross-engine reads
.config("spark.sql.iceberg.vectorization.enabled", "false")
```

Two things worth noting:

- **`io-impl = HadoopFileIO`**: Iceberg 1.7.1 ships `ADLSFileIO` which requires the Azure SDK v12 JAR. Without it, Iceberg logs a warning and falls back to `HadoopFileIO` for reads but throws `NoClassDefFoundError` in some code paths. Forcing `HadoopFileIO` explicitly avoids this entirely.
- **Vectorized reader disabled**: Snowflake writes parquet files using `DELTA_LENGTH_BYTE_ARRAY` encoding for string columns. Iceberg's vectorized reader doesn't support this — disabling it allows Spark to read files written by either engine.

### Writing Two Batches

```python
# spark_jobs/write_iceberg.py
df.writeTo("polaris.demo.sales").append()
```

Each `append()` call creates a new Iceberg snapshot in Polaris. After two batches, the table history shows:

```
+-------------------+-------------------+-----------------------+
|snapshot_id        |parent_id          |made_current_at        |
+-------------------+-------------------+-----------------------+
|6831974958144570916|NULL               |2026-07-03 01:32:32    |
|870807956582903017 |6831974958144570916|2026-07-03 01:32:33    |
+-------------------+-------------------+-----------------------+
```

```bash
make write
```

---

## Part 3 — Reading with Time Travel

Spark can read the table at any previous snapshot — useful for reproducing historical pipeline outputs:

```python
# spark_jobs/read_iceberg.py
first_snap = spark.sql(f"SELECT snapshot_id FROM {TABLE}.history ORDER BY made_current_at").first()
spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF {first_snap['snapshot_id']}").show()
```

The `.history`, `.files`, `.manifests`, and `.snapshots` metadata tables all work through the Polaris REST catalog exactly as they would with any native Iceberg catalog.

```bash
make read
```

---

## Part 4 — Iceberg Table Maintenance

Iceberg stored procedures in Spark handle all maintenance tasks:

```python
# spark_jobs/maintenance.py

# 1. Compact small files into target-size bins
spark.sql(f"""
    CALL polaris.system.rewrite_data_files(
        table    => 'demo.sales',
        strategy => 'binpack',
        options  => map('target-file-size-bytes', '134217728')
    )
""")

# 2. Consolidate manifest files after many writes
spark.sql("CALL polaris.system.rewrite_manifests(table => 'demo.sales', use_caching => true)")

# 3. Expire old snapshots — keep last 5, drop anything older than 7 days
cutoff = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
spark.sql(f"""
    CALL polaris.system.expire_snapshots(
        table       => 'demo.sales',
        older_than  => TIMESTAMP '{cutoff}',
        retain_last => 5
    )
""")

# 4. Remove orphan files from failed or aborted writes
spark.sql("CALL polaris.system.remove_orphan_files(table => 'demo.sales', older_than => ...)")
```

Two gotchas discovered during testing:

- `TIMESTAMPADD(DAY, -7, CURRENT_TIMESTAMP())` is MySQL/Snowflake syntax — Spark SQL rejects it. Use a Python-computed `TIMESTAMP 'yyyy-MM-dd HH:mm:ss'` literal instead.
- `INTERVAL N DAYS` works in regular Spark SQL but the CALL procedure parser rejects expressions with parentheses — only literals are allowed in named procedure arguments.

```bash
make maintenance
```

---

## Part 5 — Snowflake Catalog Integration

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
│              │   + adls.sas-token.*                  │
│              │     (rawdl scope, 1h TTL)             │ Azure SDK
│              │                                       ▼
│              │   3. Read/write parquet   ┌──────────────────────┐
│              │ ────────────────────────▶ │  Azure ADLS Gen2     │
└──────────────┘   using SAS token         └──────────────────────┘
```

When `ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS`, Snowflake sends `X-Iceberg-Access-Delegation: vended-credentials` on every table metadata request. Polaris responds with the table metadata **plus** a scoped Azure SAS token with `rawdl` permissions (read + add + write + delete + list). Snowflake uses this SAS token directly — **no external volume needed**.

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

Snowflake auto-resolves `OAUTH_TOKEN_URI` from the catalog's `GET /config` response — no need to specify it explicitly.

### Linked Database — Auto-Discovery

The correct syntax for a true linked database (schemas and tables auto-discovered from Polaris) is `LINKED_CATALOG`, not `CATALOG =`:

```sql
-- CORRECT — schemas and tables appear automatically
CREATE OR REPLACE DATABASE polaris_linked_db
  LINKED_CATALOG = (
    CATALOG            = 'polaris_catalog_int'
    BLOCKED_NAMESPACES = ('information_schema')
  );

-- Check sync status
SELECT SYSTEM$CATALOG_LINK_STATUS('polaris_linked_db');
-- {"executionState":"RUNNING"} → sync in progress
-- After ~30s: demo schema and sales table appear automatically
```

`CREATE DATABASE ... CATALOG = '...'` (without `LINKED_CATALOG`) creates a catalog-backed database where schemas must be registered manually. `LINKED_CATALOG` triggers a background sync that calls Polaris's namespace and table listing APIs and surfaces every namespace as a Snowflake schema.

```bash
make sf-setup
```

---

## Part 6 — Bidirectional Writes

The full test script ([`setup/03_snowflake_read_write_test.sql`](https://github.com/venkatmedida/iceberg-polaris-lakehouse/blob/main/setup/03_snowflake_read_write_test.sql)) proves every direction:

```sql
-- Phase 1: Snowflake reads Spark-written rows
SELECT * FROM polaris_linked_db.demo.sales ORDER BY order_date, order_id;
-- 6 rows from Spark ✓

-- Phase 2: Snowflake inserts
INSERT INTO polaris_linked_db.demo.sales VALUES ('ORD-SF-001', ...);

-- Phase 3: Snowflake reads its own write
SELECT COUNT(*) FROM polaris_linked_db.demo.sales;
-- 9 rows (6 Spark + 3 Snowflake) ✓

-- Phase 5: Snowflake updates a Spark-written row
MERGE INTO polaris_linked_db.demo.sales AS target
USING (...) AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET quantity = 10, amount = 249.90;
-- 0 inserted, 1 updated ✓
```

After running the Snowflake script, re-running `make read` from Spark confirms:

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

The snapshot history shows exactly 8 entries — each engine's commit is a first-class Iceberg snapshot that the other engine can time-travel to.

```bash
make sf-test   # Snowflake bidirectional test
make read      # Spark confirms it sees Snowflake's commits
```

---

## Key Learnings

**1. Realm name matters at bootstrap**
Polaris 1.5.0's default realm is `POLARIS` (uppercase). Using `default-realm` in `POLARIS_BOOTSTRAP_CREDENTIALS` causes silent `unauthorized_client` errors. Check Polaris logs — every log line includes the realm in brackets: `[requestId,POLARIS]`.

**2. Polaris 1.5.0 field names changed**
`AzureStorageConfigInfo` uses `tenantId` not `azureTenantId`. The management API returns HTTP 400 with an empty body if you use the old field name. Inspect the deployed JAR with Python's `zipfile` module if the API rejects requests without a useful error.

**3. `HadoopFileIO` over `ADLSFileIO` for Spark**
`ADLSFileIO` requires `azure-storage-file-datalake` SDK v12. Adding that JAR pulls in `DefaultAzureCredential` which tries multiple auth methods and fails loudly. Explicitly setting `io-impl = org.apache.iceberg.hadoop.HadoopFileIO` avoids the entire issue — the ABFS driver handles Azure auth through standard Spark Hadoop config.

**4. Iceberg CALL procedure syntax is strict**
Stored procedure arguments must be literals — no function calls with parentheses (so no `CAST(...)`, no `INTERVAL N DAYS`). Compute timestamps in Python and embed them as `TIMESTAMP 'yyyy-MM-dd HH:mm:ss'` literals.

**5. Credential vending supports writes on Azure**
The Polaris-vended SAS token has `sp=rawdl` scope — read, add, **write**, delete, list. No external volume is needed in Snowflake when using `ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS`. The Azure service principal lives only inside Polaris, and clients get short-lived scoped tokens per request.

**6. `LINKED_CATALOG` vs `CATALOG =`**
`CREATE DATABASE ... CATALOG = '...'` does not auto-discover schemas — it creates a catalog-backed database where tables must be registered manually. The correct syntax for a linked database that mirrors Polaris is `LINKED_CATALOG = (CATALOG = '...')`.

---

## Running It Yourself

```bash
git clone https://github.com/venkatmedida/iceberg-polaris-lakehouse
cd iceberg-polaris-lakehouse
cp .env.example .env   # fill in your credentials

make up          # start Polaris (Docker)
make setup       # bootstrap catalog + principal
make write       # Spark writes 6 rows in 2 batches
make read        # Spark reads with time travel + metadata
make maintenance # compact, rewrite manifests, expire snapshots

# Snowflake (requires snow CLI + ngrok)
ngrok http 8181              # get public URL → set POLARIS_PUBLIC_HOST in .env
make sf-setup                # create catalog integration + linked database
make sf-test                 # bidirectional read/write test
make read                    # Spark sees Snowflake's commits
```

---

*Full source: [github.com/venkatmedida/iceberg-polaris-lakehouse](https://github.com/venkatmedida/iceberg-polaris-lakehouse)*
