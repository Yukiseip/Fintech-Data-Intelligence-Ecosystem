#!/bin/bash
# Full pipeline backfill: bronze + silver for all historical dates, then gold + dbt once at the end.
# Run inside airflow worker: bash /opt/airflow/data_generator/run_pipeline_backfill.sh

set -e

BUCKET="${S3_BUCKET:-fintech-raw-data}"
SPARK_MASTER="${SPARK_MASTER_URL:-spark://spark-master:7077}"
PACKAGES="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.261,org.postgresql:postgresql:42.6.0"

SPARK_CONF="--master ${SPARK_MASTER} \
  --conf spark.executor.memory=2g \
  --conf spark.executor.cores=2 \
  --conf spark.hadoop.fs.s3a.endpoint=http://localstack:4566 \
  --conf spark.hadoop.fs.s3a.access.key=test \
  --conf spark.hadoop.fs.s3a.secret.key=test \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.sql.adaptive.enabled=true \
  --packages ${PACKAGES}"

export PYTHONPATH=/opt/spark
export POSTGRES_HOST=postgres
export POSTGRES_PORT=5432
export POSTGRES_DB=fintech_dw
export POSTGRES_USER=fintech_user
export POSTGRES_PASSWORD=fintech_secret_2026

HOUR="05"

echo "========================================"
echo " Fintech Full Pipeline Backfill"
echo " Processing 29 days of historical data"
echo "========================================"

# Step 1: Bronze + Silver for each historical day
for DAYS_AGO in $(seq 29 -1 1); do
    TARGET_DATE=$(date -u -d "${DAYS_AGO} days ago" "+%Y-%m-%d")
    echo ""
    echo "--- [Day -${DAYS_AGO}] ${TARGET_DATE} ---"

    echo "  [Bronze] Ingesting ${TARGET_DATE}/${HOUR}..."
    spark-submit ${SPARK_CONF} \
        /opt/spark/jobs/bronze_ingest.py \
        --date "${TARGET_DATE}" --hour "${HOUR}" --bucket "${BUCKET}" \
        2>&1 | grep -E "\[INFO\] __main__|ERROR|Exception" | tail -5

    echo "  [Silver] Cleansing ${TARGET_DATE}..."
    spark-submit ${SPARK_CONF} \
        /opt/spark/jobs/silver_cleanse.py \
        --date "${TARGET_DATE}" --bucket "${BUCKET}" \
        2>&1 | grep -E "\[INFO\] __main__|ERROR|Exception" | tail -5
done

echo ""
echo "========================================"
echo " Preparing dbt environment"
echo "========================================"
rm -rf /tmp/dbt_backfill && cp -a /opt/dbt /tmp/dbt_backfill
cd /tmp/dbt_backfill && dbt deps

echo ""
echo "========================================"
echo " Step 2: Gold Modeling (each day)"
echo "========================================"
for DAYS_AGO in $(seq 29 -1 1); do
    TARGET_DATE=$(date -u -d "${DAYS_AGO} days ago" "+%Y-%m-%d")
    echo "  [Gold] Modeling ${TARGET_DATE}..."
    spark-submit ${SPARK_CONF} \
        /opt/spark/jobs/gold_modeling.py \
        --date "${TARGET_DATE}" --bucket "${BUCKET}" \
        2>&1 | grep -E "\[INFO\] __main__|ERROR|Exception" | tail -5
    
    # Run dbt snapshot to capture user changes incrementally
    cd /tmp/dbt_backfill && dbt snapshot --target prod > /dev/null
done

# Run final dbt run
echo ""
echo "========================================"
echo " Step 3: dbt full refresh"
echo "========================================"
cd /tmp/dbt_backfill && dbt deps && dbt run --target prod --full-refresh

echo ""
echo "========================================"
echo " DONE! Dashboard should now have 30d data"
echo "========================================"
