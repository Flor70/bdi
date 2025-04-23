variable "aws_region" {
  description = "AWS region for resource deployment"
  type        = string
  default     = "us-east-1" # Or choose your preferred default region
}

variable "db_name" {
  description = "Name for the RDS PostgreSQL database"
  type        = string
  default     = "bdi_s8_db"
}

variable "db_username" {
  description = "Master username for the RDS PostgreSQL database"
  type        = string
  sensitive   = true # Mark as sensitive, won't show in plan output
  # No default, should be provided at runtime
}

variable "db_password" {
  description = "Master password for the RDS PostgreSQL database"
  type        = string
  sensitive   = true # Mark as sensitive, won't show in plan output
  # No default, should be provided at runtime
} 