variable "bucket_name" {
  description = "Name of the S3 bucket"
  type        = string
  default     = "fintech-raw-data"
}

variable "environment" {
  description = "Environment tag"
  type        = string
  default     = "local"
}

variable "raw_retention_days" {
  description = "Raw data lifecycle expiry in days"
  type        = number
  default     = 30
}

variable "bronze_retention_days" {
  description = "Bronze data lifecycle expiry in days"
  type        = number
  default     = 90
}
