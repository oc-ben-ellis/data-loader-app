variable "aws_region" { default = "eu-west-2" }
variable "aws_endpoint" { default = "http://localhost:4566" }
variable "aws_access_key" { default = "test" }
variable "aws_secret_key" { default = "test" }
variable "bucket_name" { default = "local-config" }
variable "queue_name" { default = "data-pipeline-orchestration-queue" }
variable "pipeline_bucket_name" { default = "oc-local-data-pipeline" }
variable "config_bucket_name" {
  description = "Name of the S3 bucket for configuration files"
  type        = string
  default     = "oc-local-data-config"
}
variable "aws_profile" {
  description = "AWS shared config/credentials profile name (optional)"
  type        = string
  default     = ""
}

variable "env" {
  description = "Deployment environment (e.g., dev, prod)"
  type        = string
  default     = "local"
}

variable "aws_use_localstack" {
  description = "If true, point AWS provider endpoints at LocalStack"
  type        = bool
  default     = false
}

variable "aws_localstack_endpoint" {
  description = "Base endpoint URL for LocalStack (no trailing slash)"
  type        = string
  default     = "http://localhost:4566"
}

# Data registry ID used by mocks (for HINTS population)
variable "data_registry_id" {
  description = "Data registry identifier for mocks (e.g., us_fl)"
  type        = string
  default     = "us_fl"
}
