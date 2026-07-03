"""
SparkSession factory for Iceberg + Polaris REST catalog + Azure ADLS Gen2.

Credential vending (default): Polaris generates scoped Azure SAS tokens
per-table and injects them into each Spark task. Spark never holds a
long-lived ADLS key — the ADLS service principal is only known to Polaris.

Direct ADLS auth (fallback): pass use_vended_credentials=False to
configure the ABFS driver with a dedicated Azure service principal.
Use this if your Polaris version doesn't support Azure vended credentials.
"""

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

# ── Polaris ───────────────────────────────────────────────────────
POLARIS_URI           = os.environ["POLARIS_URI"]
POLARIS_CLIENT_ID     = os.environ["POLARIS_CLIENT_ID"]
POLARIS_CLIENT_SECRET = os.environ["POLARIS_CLIENT_SECRET"]
POLARIS_CATALOG_NAME  = os.environ["POLARIS_CATALOG_NAME"]

# ── Azure (used only for direct ADLS auth) ────────────────────────
AZURE_STORAGE_ACCOUNT    = os.environ["AZURE_STORAGE_ACCOUNT"]
AZURE_TENANT_ID          = os.environ["AZURE_TENANT_ID"]
AZURE_SPARK_CLIENT_ID    = os.environ.get("AZURE_SPARK_CLIENT_ID", "")
AZURE_SPARK_CLIENT_SECRET = os.environ.get("AZURE_SPARK_CLIENT_SECRET", "")

# Maven coordinates — keep in sync with Makefile
_ICEBERG_PKG  = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1"
_HADOOP_AZURE = "org.apache.hadoop:hadoop-azure:3.3.4"


def build_spark_session(
    app_name: str,
    use_vended_credentials: bool = False,
) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", f"{_ICEBERG_PKG},{_HADOOP_AZURE}")

        # ── Iceberg extensions ────────────────────────────────────
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )

        # ── Polaris REST catalog registered as 'polaris' in Spark ─
        .config("spark.sql.catalog.polaris",           "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.polaris.type",      "rest")
        .config("spark.sql.catalog.polaris.uri",       POLARIS_URI)
        .config("spark.sql.catalog.polaris.credential",
                f"{POLARIS_CLIENT_ID}:{POLARIS_CLIENT_SECRET}")
        .config("spark.sql.catalog.polaris.scope",     "PRINCIPAL_ROLE:ALL")
        # 'warehouse' maps to the Polaris catalog name
        .config("spark.sql.catalog.polaris.warehouse", POLARIS_CATALOG_NAME)
        # Explicit OAuth2 token URI to avoid Iceberg auto-fallback warning
        .config("spark.sql.catalog.polaris.oauth2-server-uri",
                f"{POLARIS_URI}/v1/oauth/tokens")
        # Force HadoopFileIO so Iceberg never tries to load ADLSFileIO
        # (ADLSFileIO requires azure-storage-file-datalake SDK which we don't bundle)
        .config("spark.sql.catalog.polaris.io-impl",
                "org.apache.iceberg.hadoop.HadoopFileIO")
        # Snowflake writes parquet with DELTA_LENGTH_BYTE_ARRAY encoding for strings.
        # Iceberg's own vectorized reader doesn't support this encoding, so disable it.
        # Without this, Spark fails reading any file that Snowflake wrote.
        .config("spark.sql.iceberg.vectorization.enabled", "false")
    )

    if use_vended_credentials:
        # Request that Polaris vend per-request Azure credentials to Spark.
        # Polaris returns a short-lived SAS token via the FileIO credentials API.
        builder = builder.config(
            "spark.sql.catalog.polaris.header.X-Iceberg-Access-Delegation",
            "vended-credentials",
        )
    else:
        # Direct ABFS auth using a separate Azure service principal.
        acct    = AZURE_STORAGE_ACCOUNT
        oauth_ep = (
            f"https://login.microsoftonline.com/{AZURE_TENANT_ID}/oauth2/token"
        )
        builder = (
            builder
            .config(f"spark.hadoop.fs.azure.account.auth.type.{acct}.dfs.core.windows.net",
                    "OAuth")
            .config(f"spark.hadoop.fs.azure.account.oauth.provider.type.{acct}.dfs.core.windows.net",
                    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
            .config(f"spark.hadoop.fs.azure.account.oauth2.client.id.{acct}.dfs.core.windows.net",
                    AZURE_SPARK_CLIENT_ID)
            .config(f"spark.hadoop.fs.azure.account.oauth2.client.secret.{acct}.dfs.core.windows.net",
                    AZURE_SPARK_CLIENT_SECRET)
            .config(f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{acct}.dfs.core.windows.net",
                    oauth_ep)
        )

    return builder.getOrCreate()
