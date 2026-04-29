terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# ─── S3 Bucket ──────────────────────────────────────────────────────
resource "aws_s3_bucket" "fintech_raw_data" {
  bucket = var.bucket_name

  tags = {
    Environment = var.environment
    Project     = "fintech-analytics"
    ManagedBy   = "terraform"
  }
}

# ─── Folder prefixes: raw/ bronze/ silver/ gold/ ─────────────────
resource "aws_s3_object" "folders" {
  for_each = toset(["raw/", "bronze/", "silver/", "gold/"])

  bucket  = aws_s3_bucket.fintech_raw_data.id
  key     = each.value
  content = ""
}

# ─── Server-side encryption (AES-256, simulated in LocalStack) ────
resource "aws_s3_bucket_server_side_encryption_configuration" "sse" {
  bucket = aws_s3_bucket.fintech_raw_data.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# ─── Lifecycle policies ───────────────────────────────────────────
# NOTE: aws_s3_bucket_lifecycle_configuration hangs indefinitely on
# LocalStack Community (the PUT Lifecycle API never returns 200).
# Lifecycle rules are documented here for production reference but
# are intentionally COMMENTED OUT for local dev to keep `make infra`
# fast and non-blocking.
#
# Uncomment when targeting a real AWS account:
#
# resource "aws_s3_bucket_lifecycle_configuration" "lifecycle" {
#   bucket = aws_s3_bucket.fintech_raw_data.id
#   rule {
#     id     = "expire-raw-data"
#     status = "Enabled"
#     filter { prefix = "raw/" }
#     expiration { days = var.raw_retention_days }
#   }
#   rule {
#     id     = "expire-bronze-data"
#     status = "Enabled"
#     filter { prefix = "bronze/" }
#     expiration { days = var.bronze_retention_days }
#   }
# }
