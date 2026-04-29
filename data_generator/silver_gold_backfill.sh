#!/bin/bash
# Silver + Gold + dbt full backfill for all 29 bronze partitions.
# Run inside airflow-worker: bash /opt/airflow/data_generator/silver_gold_backfill.sh

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

echo "========================================"
echo " Step 1: Silver for all 29 days"
echo "========================================"

for DAYS_AGO in $(seq 29 -1 1); do
    TARGET_DATE=$(date -u -d "${DAYS_AGO} days ago" "+%Y-%m-%d")
    echo ""
    echo "  [Silver] ${TARGET_DATE}..."
    spark-submit ${SPARK_CONF} \
        /opt/spark/jobs/silver_cleanse.py \
        --date "${TARGET_DATE}" --bucket "${BUCKET}" \
        2>&1 | grep -E "\[INFO\] __main__|ERROR|Exception" | tail -5
done

echo ""
echo "========================================"
echo " Preparing dbt"
echo "========================================"
rm -rf /tmp/dbt_bf && cp -a /opt/dbt /tmp/dbt_bf
cd /tmp/dbt_bf && dbt deps --quiet

echo ""
echo "========================================"
echo " Step 2: Gold + dbt snapshot per day"
echo "========================================"

for DAYS_AGO in $(seq 29 -1 1); do
    TARGET_DATE=$(date -u -d "${DAYS_AGO} days ago" "+%Y-%m-%d")
    echo ""
    echo "  [Gold] ${TARGET_DATE}..."
    spark-submit ${SPARK_CONF} \
        /opt/spark/jobs/gold_modeling.py \
        --date "${TARGET_DATE}" --bucket "${BUCKET}" \
        2>&1 | grep -E "\[INFO\] __main__|ERROR|Exception" | tail -5
    cd /tmp/dbt_bf && dbt snapshot --target prod --quiet
done

echo ""
echo "========================================"
echo " Step 3: dbt run --full-refresh"
echo "========================================"
cd /tmp/dbt_bf && dbt run --target prod --full-refresh

echo ""
echo "========================================"
echo " DONE - Verifying row counts"
echo "========================================"
PGPASSWORD=fintech_secret_2026 psql -h postgres -U fintech_user -d fintech_dw -c "
SELECT
    'fact_transactions' AS table_name, count(*) AS rows FROM gold.fact_transactions
UNION ALL
SELECT 'mart_executive_kpis', count(*) FROM gold.mart_executive_kpis
UNION ALL
SELECT 'dim_users (distinct)', count(DISTINCT user_id) FROM gold.dim_users WHERE is_current = TRUE;"
