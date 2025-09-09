# S3 Bucket Outputs
output "rawdata_bucket_name" {
  description = "The name of the raw data bucket"
  value       = data.aws_s3_bucket.rawdata.bucket
}

output "curated_bucket_name" {
  description = "The name of the curated data bucket"
  value       = data.aws_s3_bucket.curated.bucket
}

output "glue_job_name" {
  description = "The name of the Glue ETL job"
  value       = aws_glue_job.etl_pipeline.name
}

output "glue_role_arn" {
  description = "The ARN of the Glue service role"
  value       = aws_iam_role.glue_role.arn
}