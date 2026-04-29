terraform {
  required_version = ">= 1.7"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# ─── LocalStack provider ────────────────────────────────────────────
provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3  = "http://localstack:4566"
    iam = "http://localstack:4566"
  }

  # LocalStack requires path-style S3 URLs (virtual-hosted not supported)
  s3_use_path_style = true
}

# ─── Modules ────────────────────────────────────────────────────────
module "s3" {
  source = "./modules/s3"

  bucket_name          = var.bucket_name
  environment          = var.environment
  raw_retention_days   = var.raw_retention_days
  bronze_retention_days = var.bronze_retention_days
}

module "iam" {
  source = "./modules/iam"
}
