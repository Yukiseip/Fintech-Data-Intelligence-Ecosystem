#!/bin/sh
# init-localstack.sh
# Wait for LocalStack S3 to be available, then run Terraform to provision infrastructure.
# Runs inside the 'terraform' service container (hashicorp/terraform:1.7 / Alpine).
# Called by: make infra → docker compose run --rm terraform

set -eu

TERRAFORM_DIR="/infrastructure/terraform"
LOCALSTACK_URL="http://localstack:4566"
MAX_RETRIES=30
RETRY_INTERVAL=5

echo "══════════════════════════════════════════════════"
echo "  Fintech Analytics — LocalStack Infrastructure"
echo "══════════════════════════════════════════════════"

# ── Wait for LocalStack S3 ────────────────────────────────────────
echo "⏳ Waiting for LocalStack S3 to be available..."
retries=0
until wget -qO- "${LOCALSTACK_URL}/_localstack/health" 2>/dev/null | grep -q '"s3"'; do
  retries=$((retries + 1))
  if [ "$retries" -ge "$MAX_RETRIES" ]; then
    echo "❌ LocalStack S3 not available after ${MAX_RETRIES} retries. Exiting."
    exit 1
  fi
  echo "   Attempt ${retries}/${MAX_RETRIES} — waiting ${RETRY_INTERVAL}s..."
  sleep "${RETRY_INTERVAL}"
done

echo "✅ LocalStack S3 is available."

# ── Terraform Init + Apply ────────────────────────────────────────
if [ ! -d "${TERRAFORM_DIR}" ]; then
  echo "❌ Terraform directory not found at ${TERRAFORM_DIR}"
  exit 1
fi

cd "${TERRAFORM_DIR}"

echo "🔧 Initializing Terraform..."
terraform init -upgrade -no-color

echo "📋 Planning infrastructure..."
terraform plan -no-color

echo "🚀 Applying Terraform configuration..."
terraform apply -auto-approve -no-color

echo ""
echo "✅ Infrastructure provisioned successfully!"
BUCKET=$(terraform output -raw bucket_name 2>/dev/null || echo "fintech-raw-data")
echo "   Bucket: ${BUCKET}"
echo ""

# ── Verify bucket via LocalStack health endpoint ──────────────────
echo "🔍 Verifying S3 bucket via LocalStack..."
if wget -qO- "${LOCALSTACK_URL}/_localstack/health" | grep -q '"s3"'; then
  echo "✅ LocalStack S3 service confirmed healthy."
else
  echo "⚠️  Could not verify LocalStack health endpoint."
fi

echo "══════════════════════════════════════════════════"
echo "  Infrastructure setup complete!"
echo "══════════════════════════════════════════════════"
