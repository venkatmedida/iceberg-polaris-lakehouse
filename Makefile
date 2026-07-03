.PHONY: up down logs setup write read maintenance \
        sf-setup sf-setup-volume sf-test

# ── Spark packages downloaded at runtime from Maven Central ──────
ICEBERG_PKG  := org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1
HADOOP_AZURE := org.apache.hadoop:hadoop-azure:3.3.4
SPARK_PKGS   := $(ICEBERG_PKG),$(HADOOP_AZURE)

SPARK_OPTS := \
  --packages $(SPARK_PKGS) \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

# ── Docker stack ─────────────────────────────────────────────────
up:
	docker compose up -d
	@echo "Polaris ready at http://localhost:8181"

down:
	docker compose down

logs:
	docker compose logs -f polaris

# ── Polaris bootstrap ─────────────────────────────────────────────
setup:
	python setup/01_setup_polaris.py

# ── Spark jobs ───────────────────────────────────────────────────
write:
	spark-submit $(SPARK_OPTS) spark_jobs/write_iceberg.py

read:
	spark-submit $(SPARK_OPTS) spark_jobs/read_iceberg.py

maintenance:
	spark-submit $(SPARK_OPTS) spark_jobs/maintenance.py

# ── Snowflake setup (requires: snow CLI + MJ07903 connection) ────
# sf-setup      : catalog integration + linked database (vended credentials)
# sf-setup-volume: optional external volume (only for EXTERNAL_VOLUME_CREDENTIALS mode)
# sf-test       : bidirectional read/write interoperability test
SF_CONN := MJ07903

sf-setup:
	snow sql -c $(SF_CONN) -f setup/02b_snowflake_catalog_linked_db.sql

sf-setup-volume:
	snow sql -c $(SF_CONN) -f setup/02a_snowflake_volume_oauth.sql

sf-test:
	snow sql -c $(SF_CONN) -f setup/03_snowflake_read_write_test.sql
