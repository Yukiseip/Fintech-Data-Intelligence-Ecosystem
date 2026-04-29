variable "bucket_name" {
  description = "Name of the S3 bucket for raw data"
  type        = string
  default     = "fintech-raw-data"
}

variable "environment" {
  description = "Deployment environment (local, dev, prod)"
  type        = string
  default     = "local"
}

variable "raw_retention_days" {
  description = "Days before raw/ prefix files expire"
  type        = number
  default     = 30
}

variable "bronze_retention_days" {
  description = "Days before bronze/ prefix files expire"
  type        = number
  default     = 90
}
