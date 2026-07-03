-- ================================================================
-- Snowflake Horizon Catalog: wire Polaris + ADLS so Snowflake can
-- read the same Iceberg tables that Spark writes.
--
-- Approach: credential vending (VENDED_CREDENTIALS)
--   Polaris mints short-lived Azure SAS tokens per request, so no
--   long-lived ADLS keys are stored in Snowflake. Every namespace
--   and table in Polaris is auto-discovered via LINKED_CATALOG.
--
-- Replace every <PLACEHOLDER> before running.
-- Run as ACCOUNTADMIN.
-- ================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE <SNOWFLAKE_WAREHOUSE>;

-- ── Step 1: Catalog integration with credential vending ──────────
-- POLARIS_PUBLIC_HOST = ngrok / Tailscale URL exposing port 8181.
-- POLARIS_CATALOG_NAME = the Polaris catalog name (maps to warehouse).
-- ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS tells Snowflake to
--   send X-Iceberg-Access-Delegation: vended-credentials on every
--   table request; Polaris responds with a scoped Azure SAS token.
CREATE OR REPLACE CATALOG INTEGRATION polaris_catalog_int
  CATALOG_SOURCE    = ICEBERG_REST
  TABLE_FORMAT      = ICEBERG
  CATALOG_NAMESPACE = '<POLARIS_NAMESPACE>'
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

-- Verify: REST_CONFIG should show ACCESS_DELEGATION_MODE=VENDED_CREDENTIALS
-- and REST_AUTHENTICATION should show the auto-resolved OAUTH_TOKEN_URI
DESC CATALOG INTEGRATION polaris_catalog_int;

-- ── Step 2: Linked database — auto-discovers all Polaris namespaces ──
-- LINKED_CATALOG (not CATALOG =) is the correct syntax for a linked
-- database. Snowflake syncs every namespace and table from Polaris
-- automatically. New Spark-written tables appear within the catalog
-- integration refresh interval (default 30 s, set via REFRESH_INTERVAL_SECS).
-- No EXTERNAL_VOLUME is needed — Polaris vends SAS tokens for both
-- reads and writes (rawdl scope: read + add + write + delete + list).
CREATE OR REPLACE DATABASE polaris_linked_db
  LINKED_CATALOG = (
    CATALOG            = 'polaris_catalog_int'
    BLOCKED_NAMESPACES = ('information_schema')
  );

-- Check sync status — wait for executionState to leave RUNNING
SELECT SYSTEM$CATALOG_LINK_STATUS('polaris_linked_db');

-- ── Step 3: Query Iceberg data ────────────────────────────────────
USE DATABASE polaris_linked_db;
USE SCHEMA <POLARIS_NAMESPACE>;

SELECT * FROM sales ORDER BY order_date, order_id;

SELECT
    order_date,
    COUNT(*)    AS orders,
    SUM(amount) AS total_revenue
FROM sales
GROUP BY order_date
ORDER BY order_date;

-- Iceberg metadata (snapshot, metadata file path) via Horizon functions
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('sales');

-- ── Step 4: Grant read access to analysts ────────────────────────
GRANT USAGE  ON DATABASE polaris_linked_db TO ROLE <ANALYST_ROLE>;
GRANT USAGE  ON ALL SCHEMAS IN DATABASE polaris_linked_db TO ROLE <ANALYST_ROLE>;
GRANT SELECT ON ALL TABLES  IN DATABASE polaris_linked_db TO ROLE <ANALYST_ROLE>;
-- Auto-grant future tables added by Spark
GRANT SELECT ON FUTURE TABLES IN ALL SCHEMAS IN DATABASE polaris_linked_db TO ROLE <ANALYST_ROLE>;

-- ── (Optional) External volume — NOT needed with vended credentials ──
-- With ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS, Polaris mints
-- Azure SAS tokens with rawdl scope (read + add + write + delete + list).
-- Snowflake gets the table's base location from the Polaris table
-- metadata and uses the SAS token to write parquet files directly —
-- no external volume required for reads OR writes.
--
-- Create an external volume only if you switch to
-- ACCESS_DELEGATION_MODE = EXTERNAL_VOLUME_CREDENTIALS (Snowflake
-- uses its own managed identity instead of Polaris-vended tokens) or
-- for Snowflake-managed Iceberg tables (not backed by Polaris).
--
-- CREATE OR REPLACE EXTERNAL VOLUME polaris_adls_volume
--   STORAGE_LOCATIONS = (
--     (
--       NAME             = 'polaris-adls-loc'
--       STORAGE_PROVIDER = 'AZURE'
--       STORAGE_BASE_URL = 'azure://<AZURE_STORAGE_ACCOUNT>.blob.core.windows.net/<AZURE_CONTAINER>/'
--       AZURE_TENANT_ID  = '<AZURE_TENANT_ID>'
--     )
--   );
-- DESC EXTERNAL VOLUME polaris_adls_volume;
-- -- Visit AZURE_CONSENT_URL from DESC output to register Snowflake's
-- -- managed identity in your tenant, then grant it
-- -- Storage Blob Data Contributor on the container in Azure IAM.
