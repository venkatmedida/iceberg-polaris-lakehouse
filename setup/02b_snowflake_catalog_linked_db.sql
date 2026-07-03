-- ================================================================
-- Phase B: Catalog integration + linked database + queries
--
-- 02a (external volume) is OPTIONAL with this approach:
-- ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS lets Polaris mint
-- short-lived Azure SAS tokens (rawdl scope) for both reads and
-- writes — no Snowflake-managed identity or external volume needed.
-- Only run 02a if you need EXTERNAL_VOLUME_CREDENTIALS mode or
-- Snowflake-managed Iceberg tables outside Polaris.
-- ================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- ── Step 3: Catalog integration with credential vending ──────────
-- Polaris vends short-lived Azure SAS tokens (rawdl scope, 1h TTL)
-- per table request via X-Iceberg-Access-Delegation: vended-credentials.
-- Snowflake auto-resolves OAUTH_TOKEN_URI from the catalog config endpoint.
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

DESC CATALOG INTEGRATION polaris_catalog_int;

-- ── Step 4: Linked database — auto-discovers all Polaris namespaces ──
-- LINKED_CATALOG triggers a background sync: Snowflake calls Polaris's
-- namespace and table listing APIs, then surfaces every namespace as a
-- schema and every Iceberg table as a native Snowflake table.
-- No EXTERNAL_VOLUME needed — Polaris vends SAS tokens for both
-- reads and writes (rawdl scope: read + add + write + delete + list).
CREATE OR REPLACE DATABASE polaris_linked_db
  LINKED_CATALOG = (
    CATALOG            = 'polaris_catalog_int'
    BLOCKED_NAMESPACES = ('information_schema')
  );

-- executionState: RUNNING → sync in progress; poll until done
SELECT SYSTEM$CATALOG_LINK_STATUS('polaris_linked_db');

-- ── Step 5: Query auto-discovered Iceberg data ───────────────────
USE DATABASE polaris_linked_db;
USE SCHEMA demo;

SELECT * FROM sales ORDER BY order_date, order_id;

SELECT
    order_date,
    COUNT(*)    AS orders,
    SUM(amount) AS total_revenue
FROM sales
GROUP BY order_date
ORDER BY order_date;

SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('sales');
