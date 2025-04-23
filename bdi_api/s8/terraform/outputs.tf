output "s3_bucket_name" {
  description = "The name of the created S3 data lake bucket"
  value       = aws_s3_bucket.data_lake.bucket
}

output "rds_instance_endpoint" {
  description = "The connection endpoint for the RDS PostgreSQL instance"
  value       = aws_db_instance.postgres_db.endpoint
  sensitive   = false # Endpoint is usually not sensitive, but consider your security policy
}

output "rds_instance_port" {
  description = "The connection port for the RDS PostgreSQL instance"
  value       = aws_db_instance.postgres_db.port
}

output "rds_instance_db_name" {
  description = "The database name for the RDS PostgreSQL instance"
  value       = aws_db_instance.postgres_db.db_name
}

output "rds_security_group_id" {
  description = "The ID of the security group associated with the RDS instance"
  value       = aws_security_group.rds_sg.id
} 