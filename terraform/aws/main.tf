terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Uncomment for remote state
  # backend "s3" {
  #   bucket         = "your-terraform-state-bucket"
  #   key            = "de-infrastructure/terraform.tfstate"
  #   region         = "us-east-1"
  #   dynamodb_table = "terraform-state-lock"
  #   encrypt        = true
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Environment = var.environment
      Project     = "DataEngineering"
      ManagedBy   = "Terraform"
    }
  }
}

# S3 Buckets for Data Lake
resource "aws_s3_bucket" "data_lake_raw" {
  bucket = "${var.project_name}-data-lake-raw-${var.environment}"

  tags = {
    Name = "Raw Data Lake"
    Layer = "bronze"
  }
}

resource "aws_s3_bucket" "data_lake_processed" {
  bucket = "${var.project_name}-data-lake-processed-${var.environment}"

  tags = {
    Name = "Processed Data Lake"
    Layer = "silver"
  }
}

resource "aws_s3_bucket" "data_lake_curated" {
  bucket = "${var.project_name}-data-lake-curated-${var.environment}"

  tags = {
    Name = "Curated Data Lake"
    Layer = "gold"
  }
}

# S3 Bucket Versioning
resource "aws_s3_bucket_versioning" "data_lake_raw" {
  bucket = aws_s3_bucket.data_lake_raw.id

  versioning_configuration {
    status = "Enabled"
  }
}

# S3 Lifecycle Rules
resource "aws_s3_bucket_lifecycle_configuration" "data_lake_raw" {
  bucket = aws_s3_bucket.data_lake_raw.id

  rule {
    id     = "archive-old-data"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    expiration {
      days = 365
    }
  }
}

# S3 Encryption
resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake_raw" {
  bucket = aws_s3_bucket.data_lake_raw.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Glue Catalog Database
resource "aws_glue_catalog_database" "data_warehouse" {
  name        = "${var.project_name}_data_warehouse_${var.environment}"
  description = "Data warehouse for analytics"

  location_uri = "s3://${aws_s3_bucket.data_lake_curated.bucket}/"
}

# IAM Role for Glue
resource "aws_iam_role" "glue_service_role" {
  name = "${var.project_name}-glue-service-role-${var.environment}"

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

resource "aws_iam_role_policy_attachment" "glue_service_policy" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# S3 Access Policy for Glue
resource "aws_iam_role_policy" "glue_s3_access" {
  name = "glue-s3-access"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "${aws_s3_bucket.data_lake_raw.arn}/*",
          "${aws_s3_bucket.data_lake_processed.arn}/*",
          "${aws_s3_bucket.data_lake_curated.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data_lake_raw.arn,
          aws_s3_bucket.data_lake_processed.arn,
          aws_s3_bucket.data_lake_curated.arn
        ]
      }
    ]
  })
}

# Glue Crawler for Raw Data
resource "aws_glue_crawler" "raw_data_crawler" {
  name          = "${var.project_name}-raw-data-crawler-${var.environment}"
  role          = aws_iam_role.glue_service_role.arn
  database_name = aws_glue_catalog_database.data_warehouse.name

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake_raw.bucket}/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = {
        AddOrUpdateBehavior = "InheritFromTable"
      }
    }
  })
}

# Lambda Execution Role
resource "aws_iam_role" "lambda_execution_role" {
  name = "${var.project_name}-lambda-execution-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Kinesis Data Stream for Real-time Processing
resource "aws_kinesis_stream" "event_stream" {
  name             = "${var.project_name}-event-stream-${var.environment}"
  shard_count      = 1
  retention_period = 24

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = {
    Name = "Event Stream"
  }
}

# EventBridge Rule for Scheduled Jobs
resource "aws_cloudwatch_event_rule" "daily_etl" {
  name                = "${var.project_name}-daily-etl-${var.environment}"
  description         = "Trigger daily ETL job"
  schedule_expression = "cron(0 2 * * ? *)"  # Daily at 2 AM UTC

  tags = {
    Name = "Daily ETL Trigger"
  }
}

# SNS Topic for Alerts
resource "aws_sns_topic" "data_pipeline_alerts" {
  name = "${var.project_name}-data-pipeline-alerts-${var.environment}"

  tags = {
    Name = "Data Pipeline Alerts"
  }
}

# CloudWatch Log Group for Data Pipeline
resource "aws_cloudwatch_log_group" "data_pipeline" {
  name              = "/aws/data-pipeline/${var.project_name}-${var.environment}"
  retention_in_days = 30

  tags = {
    Name = "Data Pipeline Logs"
  }
}
