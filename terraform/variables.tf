variable "aws_access_key" {
  description = "AWS access key"
  type        = string
  sensitive   = true
}

variable "aws_secret_key" {
  description = "AWS secret key"
  type        = string
  sensitive   = true
}

variable "aws_session_token" {
  description = "AWS session token"
  type        = string
  sensitive   = true
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "s3_bucket" {
  description = "S3 bucket name"
  type        = string
  default     = "bdi-aircraft-floriano"
}

variable "ami_id" {
  description = "AMI ID for EC2 instance"
  type        = string
  default     = "ami-0c7217cdde317cfec" # Ubuntu 22.04 LTS in us-east-1
}

variable "public_key_path" {
  description = "Path to the public key for SSH access"
  type        = string
  default     = "~/.ssh/id_rsa.pub"
}
