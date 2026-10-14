# Fill these in with your own AWS details before running terraform apply.
# Nothing here is real — just placeholders so the shape is clear.

variable "aws_region" {
  description = "AWS region for the MSK cluster and buckets"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Prefix used for resource names"
  type        = string
  default     = "clickstream-pipeline"
}

variable "vpc_id" {
  description = "VPC where MSK will live"
  type        = string
  default     = "vpc-PLACEHOLDER"
}

variable "subnet_ids" {
  description = "Private subnets for MSK brokers (need at least 2 AZs)"
  type        = list(string)
  default     = ["subnet-PLACEHOLDER-1", "subnet-PLACEHOLDER-2"]
}

variable "kafka_version" {
  description = "Kafka version for the MSK cluster"
  type        = string
  default     = "3.6.0"
}

variable "broker_instance_type" {
  description = "MSK broker instance type"
  type        = string
  default     = "kafka.m5.large"
}

variable "bronze_bucket_name" {
  description = "S3 bucket for bronze (raw) output"
  type        = string
  default     = "PLACEHOLDER-bronze-bucket"
}

variable "silver_bucket_name" {
  description = "S3 bucket for silver (cleaned) output"
  type        = string
  default     = "PLACEHOLDER-silver-bucket"
}

variable "checkpoint_bucket_name" {
  description = "S3 bucket for Spark checkpoints"
  type        = string
  default     = "PLACEHOLDER-checkpoint-bucket"
}
