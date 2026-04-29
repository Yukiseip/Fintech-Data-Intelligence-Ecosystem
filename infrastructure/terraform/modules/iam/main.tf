# IAM least-privilege roles for LocalStack simulation
# In LocalStack, IAM is simulated — this creates roles for documentation purposes

resource "aws_iam_role" "spark_role" {
  name = "fintech-spark-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })

  tags = {
    Project = "fintech-analytics"
  }
}

resource "aws_iam_role_policy" "spark_s3_policy" {
  name = "fintech-spark-s3-policy"
  role = aws_iam_role.spark_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"]
        Resource = ["arn:aws:s3:::fintech-raw-data", "arn:aws:s3:::fintech-raw-data/*"]
      }
    ]
  })
}
