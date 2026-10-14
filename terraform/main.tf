# Terraform for the AWS side of the pipeline:
# MSK cluster, S3 buckets for bronze/silver/checkpoints, and IAM roles.
# All identifiers are placeholders — replace with your real VPC/subnets/buckets.

terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# --- S3 buckets -------------------------------------------------------------

resource "aws_s3_bucket" "bronze" {
  bucket = var.bronze_bucket_name
  tags   = { Name = "${var.project_name}-bronze" }
}

resource "aws_s3_bucket" "silver" {
  bucket = var.silver_bucket_name
  tags   = { Name = "${var.project_name}-silver" }
}

resource "aws_s3_bucket" "checkpoints" {
  bucket = var.checkpoint_bucket_name
  tags   = { Name = "${var.project_name}-checkpoints" }
}

resource "aws_s3_bucket_versioning" "checkpoints" {
  bucket = aws_s3_bucket.checkpoints.id
  versioning_configuration {
    status = "Enabled"
  }
}

# --- MSK cluster ------------------------------------------------------------

resource "aws_msk_cluster" "main" {
  cluster_name           = "${var.project_name}-msk"
  kafka_version          = var.kafka_version
  number_of_broker_nodes = 3

  broker_node_group_info {
    instance_type   = var.broker_instance_type
    client_subnets  = var.subnet_ids
    security_groups = [aws_security_group.msk.id]
  }

  client_authentication {
    sasl {
      iam   = true
      scram = false
    }
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "TLS"
    }
  }

  tags = { Project = var.project_name }
}

resource "aws_security_group" "msk" {
  name        = "${var.project_name}-msk-sg"
  description = "Allow Kafka traffic from Spark / producers"
  vpc_id      = var.vpc_id

  # Tighten these to your producer / EMR / EKS security groups in real life.
  ingress {
    from_port   = 9098
    to_port     = 9098
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/8"]
    description = "MSK IAM auth"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# --- IAM role for the Spark job ---------------------------------------------

resource "aws_iam_role" "spark_job" {
  name = "${var.project_name}-spark-job-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "spark_job" {
  name = "${var.project_name}-spark-job-policy"
  role = aws_iam_role.spark_job.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:DescribeTopic",
          "kafka-cluster:ReadData",
          "kafka-cluster:WriteData",
          "kafka-cluster:DescribeGroup",
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
        Resource = [
          aws_s3_bucket.bronze.arn,
          "${aws_s3_bucket.bronze.arn}/*",
          aws_s3_bucket.silver.arn,
          "${aws_s3_bucket.silver.arn}/*",
          aws_s3_bucket.checkpoints.arn,
          "${aws_s3_bucket.checkpoints.arn}/*",
        ]
      },
    ]
  })
}

output "msk_bootstrap_brokers_iam" {
  description = "Use this as KAFKA_BOOTSTRAP_SERVERS for the Spark job and producer"
  value       = aws_msk_cluster.main.bootstrap_brokers_sasl_iam
}
