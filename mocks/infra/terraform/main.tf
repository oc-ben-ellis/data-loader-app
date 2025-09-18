terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile != "" ? var.aws_profile : null
  s3_use_path_style          = var.aws_use_localstack
  skip_credentials_validation = var.aws_use_localstack
  skip_metadata_api_check     = var.aws_use_localstack
  skip_requesting_account_id  = var.aws_use_localstack

  access_key = var.aws_use_localstack ? var.aws_access_key : null
  secret_key = var.aws_use_localstack ? var.aws_secret_key : null

  # If running against LocalStack, set endpoints per service
  endpoints {
    s3  = var.aws_use_localstack ? var.aws_localstack_endpoint : null
    sqs = var.aws_use_localstack ? var.aws_localstack_endpoint : null
  }
}

resource "aws_sqs_queue" "pipeline" {
  name = var.queue_name
}

output "pipeline_bucket_name" { value = aws_s3_bucket.pipeline.bucket }
output "config_bucket_name" { value = aws_s3_bucket.config.bucket }
output "queue_url"   { value = aws_sqs_queue.pipeline.id }
output "region" { value = var.aws_region }
output "data_registry_id" { value = var.data_registry_id }

resource "aws_s3_bucket" "pipeline" {
  bucket = var.pipeline_bucket_name
}

resource "aws_s3_bucket" "config" {
  bucket = var.config_bucket_name
}
