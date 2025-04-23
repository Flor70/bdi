terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  required_version = ">= 1.0"
}

provider "aws" {
  region = var.aws_region
}

# Fetch AWS Account ID and Region using data sources
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# Generate a unique suffix based on Account ID and Region for resource names
locals {
  account_id    = data.aws_caller_identity.current.account_id
  aws_region    = data.aws_region.current.name
  unique_suffix = substr(local.account_id, -4, 4) # Last 4 digits of account ID
  resource_tags = {
    Project     = "BDI S8 Exercise"
    Environment = "Development"
    ManagedBy   = "Terraform"
  }
}

# S3 Bucket for Data Lake
resource "aws_s3_bucket" "data_lake" {
  bucket = "bdi-s8-data-lake-${local.unique_suffix}" # Add unique suffix

  tags = local.resource_tags
}

# Apply block public access settings
resource "aws_s3_bucket_public_access_block" "data_lake_public_access" {
  bucket = aws_s3_bucket.data_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# RDS PostgreSQL Instance
# Note: This creates a publicly accessible instance for simplicity in the exercise.
# In a real-world scenario, use private subnets and stricter security groups.
resource "aws_db_instance" "postgres_db" {
  identifier           = "bdi-s8-db-${local.unique_suffix}"
  allocated_storage    = 20 # Minimum storage in GB
  engine               = "postgres"
  engine_version       = "15" # Specify desired PostgreSQL version
  instance_class       = "db.t3.micro" # Small instance type (consider free tier eligibility)
  db_name              = var.db_name
  username             = var.db_username
  password             = var.db_password
  skip_final_snapshot  = true
  publicly_accessible  = true # For exercise simplicity
  vpc_security_group_ids = [aws_security_group.rds_sg.id] # Attach security group

  tags = local.resource_tags
}

# Security Group for RDS allowing PostgreSQL access (from anywhere for exercise simplicity)
resource "aws_security_group" "rds_sg" {
  name        = "bdi-s8-rds-sg-${local.unique_suffix}"
  description = "Allow PostgreSQL access from anywhere (Exercise Only)"

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"] # WARNING: Allows access from any IP. Not recommended for production.
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(local.resource_tags, { Name = "bdi-s8-rds-sg-${local.unique_suffix}" })
} 