#!/bin/bash
# Backfill 30 days of historical data for the dashboard
# Run from inside the airflow container:
#   bash /opt/airflow/data_generator/backfill_history.sh

set -e

ENDPOINT="${LOCALSTACK_ENDPOINT:-http://localstack:4566}"
BUCKET="${S3_BUCKET:-fintech-raw-data}"

echo "=== Fintech Historical Backfill ==="
echo "Generating 30 days of historical data..."

cd /opt/airflow

for DAYS_AGO in $(seq 29 -1 1); do
    TARGET_DATE=$(date -u -d "${DAYS_AGO} days ago" "+%Y-%m-%dT%H:00:00" 2>/dev/null \
                  || date -u -v-${DAYS_AGO}d "+%Y-%m-%dT%H:00:00")
    echo "[Day -${DAYS_AGO}] Generating data for ${TARGET_DATE}..."
    LOCALSTACK_ENDPOINT="${ENDPOINT}" S3_BUCKET="${BUCKET}" \
    AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1 \
    python -m data_generator.main generate all --execution-date "${TARGET_DATE}"
done

echo ""
echo "=== Backfill complete! Now run dbt to rebuild the marts ==="
