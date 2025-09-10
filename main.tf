# Reference existing S3 buckets
data "aws_s3_bucket" "rawdata" {
  bucket = "salesforce-objects-rawzone-01"
}

data "aws_s3_bucket" "curated" {
  bucket = "curated-bc-01"
}

# Upload ETL script to S3
resource "aws_s3_object" "etl_script" {
  bucket = data.aws_s3_bucket.rawdata.bucket
  key    = "scripts/etl_pipeline.py"
  source = "${path.module}/etl_pipeline.py"
  etag   = filemd5("${path.module}/etl_pipeline.py")
}

# Random suffix for unique naming
resource "random_id" "suffix" {
  byte_length = 4
}

# IAM Role for Glue Job
resource "aws_iam_role" "glue_role" {
  name                  = "glue-etl-role-${random_id.suffix.hex}"
  force_detach_policies = true

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_policy" {
  name = "glue-s3-policy"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          "${data.aws_s3_bucket.rawdata.arn}",
          "${data.aws_s3_bucket.rawdata.arn}/*",
          "${data.aws_s3_bucket.curated.arn}",
          "${data.aws_s3_bucket.curated.arn}/*"
        ]
      }
    ]
  })
}

# Glue Job
resource "aws_glue_job" "etl_pipeline" {
  name              = "etl-pipeline-job-${random_id.suffix.hex}"
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = "5.0"
  number_of_workers = 2
  worker_type       = "G.1X"

  command {
    script_location = "s3://${data.aws_s3_bucket.rawdata.bucket}/scripts/etl_pipeline.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"        = "python"
    "--job-bookmark-option" = "job-bookmark-enable"
    "--enable-metrics"      = ""
  }

  execution_property {
    max_concurrent_runs = 1
  }

  max_retries = 0
  timeout     = 60

  depends_on = [
    aws_s3_object.etl_script,
    aws_iam_role_policy_attachment.glue_service_role,
    aws_iam_role_policy.glue_s3_policy
  ]

  lifecycle {
    prevent_destroy = false
  }
}
