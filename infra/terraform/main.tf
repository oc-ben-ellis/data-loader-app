terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
    docker = {
      source  = "kreuzwerker/docker"
      version = ">= 3.0.2"
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
    sts = var.aws_use_localstack ? var.aws_localstack_endpoint : null
    iam = var.aws_use_localstack ? var.aws_localstack_endpoint : null
    elasticache = var.aws_use_localstack ? var.aws_localstack_endpoint : null
  }
}

###############################################
# Local Docker Redis for functional test runs #
###############################################

provider "docker" {}

resource "docker_image" "redis" {
  name         = "redis:7-alpine"
  keep_locally = true
}

resource "docker_container" "redis" {
  name  = "loader-functional-redis"
  image = docker_image.redis.image_id

  # Always restart during the test session if it crashes
  restart = "unless-stopped"

  # Expose Redis on the host so tests (running on host) can connect
  ports {
    internal = 6379
    # Use 0 to have Docker assign a random available host port
    external = 0
  }
}

output "redis_host" {
  value = "localhost"
  depends_on = [docker_container.redis]
}

output "redis_port" {
  # Report the actual published host port assigned by Docker
  value = try(docker_container.redis.ports[0].external, 0)
  depends_on = [docker_container.redis]
}

# Expose selected values for test-env-up HINTS
output "aws_region_out" { value = var.aws_region }
