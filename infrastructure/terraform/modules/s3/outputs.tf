output "bucket_name" {
  description = "S3 bucket name"
  value       = aws_s3_bucket.fintech_raw_data.id
}

output "bucket_arn" {
  description = "S3 bucket ARN"
  value       = aws_s3_bucket.fintech_raw_data.arn
}
