-- ================================================================
-- Phase A: External volume — OPTIONAL with credential vending
--
-- With ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS (the approach
-- used in 02b), Polaris mints Azure SAS tokens with rawdl scope
-- (read + add + write + delete + list). Snowflake reads table
-- location from Polaris metadata and uses the vended token to
-- access ADLS directly — no external volume needed for reads or
-- writes via the LINKED_CATALOG.
--
-- Create an external volume only if you need:
--   • ACCESS_DELEGATION_MODE = EXTERNAL_VOLUME_CREDENTIALS
--     (Snowflake uses its own managed identity instead of
--     Polaris-vended tokens)
--   • Snowflake-managed Iceberg tables not backed by Polaris
--
-- If you do create it, run the steps below BEFORE 02b, then grant
-- the identity in AZURE_MULTI_TENANT_APP_NAME Storage Blob Data
-- Contributor on the <AZURE_CONTAINER> container in Azure IAM.
-- Visit AZURE_CONSENT_URL if the identity isn't in your tenant yet.
-- ================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE EXTERNAL VOLUME polaris_adls_volume
  STORAGE_LOCATIONS = (
    (
      NAME             = 'polaris-adls-loc'
      STORAGE_PROVIDER = 'AZURE'
      STORAGE_BASE_URL = 'azure://<AZURE_STORAGE_ACCOUNT>.blob.core.windows.net/<AZURE_CONTAINER>/'
      AZURE_TENANT_ID  = '<AZURE_TENANT_ID>'
    )
  );

-- Copy AZURE_MULTI_TENANT_APP_NAME and AZURE_CONSENT_URL from this output
DESC EXTERNAL VOLUME polaris_adls_volume;
