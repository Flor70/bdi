output "public_ip" {
  description = "Public IP of EC2 instance"
  value       = aws_instance.api.public_ip
}

output "api_url" {
  description = "URL for the API"
  value       = "http://${aws_instance.api.public_ip}:8080"
}
