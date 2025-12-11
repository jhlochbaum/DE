output "raw_data_bucket" {
  description = "S3 bucket for raw data"
  value       = aws_s3_bucket.data_lake_raw.id
}

output "processed_data_bucket" {
  description = "S3 bucket for processed data"
  value       = aws_s3_bucket.data_lake_processed.id
}

output "curated_data_bucket" {
  description = "S3 bucket for curated data"
  value       = aws_s3_bucket.data_lake_curated.id
}

output "glue_database_name" {
  description = "Glue catalog database name"
  value       = aws_glue_catalog_database.data_warehouse.name
}

output "glue_crawler_name" {
  description = "Glue crawler name"
  value       = aws_glue_crawler.raw_data_crawler.name
}

output "kinesis_stream_name" {
  description = "Kinesis data stream name"
  value       = aws_kinesis_stream.event_stream.name
}

output "kinesis_stream_arn" {
  description = "Kinesis data stream ARN"
  value       = aws_kinesis_stream.event_stream.arn
}

output "sns_topic_arn" {
  description = "SNS topic ARN for alerts"
  value       = aws_sns_topic.data_pipeline_alerts.arn
}

output "glue_role_arn" {
  description = "IAM role ARN for Glue"
  value       = aws_iam_role.glue_service_role.arn
}

output "lambda_role_arn" {
  description = "IAM role ARN for Lambda"
  value       = aws_iam_role.lambda_execution_role.arn
}
