variable "aws_region" { default = "eu-west-2" }
variable "aws_endpoint" { default = "http://localhost:4566" }
variable "aws_access_key" { default = "test" }
variable "aws_secret_key" { default = "test" }
variable "aws_profile" {
  description = "AWS shared config/credentials profile name (optional)"
  type        = string
  default     = ""
}
variable "aws_use_localstack" {
  type    = bool
  default = true
}

variable "aws_localstack_endpoint" {
  type    = string
  default = "http://localhost:4566"
}

variable "env" {
  description = "Deployment environment (e.g., dev, prod)"
  type        = string
  default     = "local"
}

