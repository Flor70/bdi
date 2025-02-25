#!/bin/bash

# Enable logging
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

echo "[$(date)] Starting initialization script..."

# Update and install Docker
echo "[$(date)] Updating system and installing dependencies..."
yum update -y
yum install -y docker git

# Start Docker service
echo "[$(date)] Starting Docker service..."
systemctl start docker
systemctl enable docker

# Create environment file
echo "[$(date)] Creating environment file..."
mkdir -p /root
cat > /root/.env << EOL
AWS_ACCESS_KEY_ID=${aws_access_key}
AWS_SECRET_ACCESS_KEY=${aws_secret_key}
AWS_SESSION_TOKEN=${aws_session_token}
AWS_REGION=${aws_region}
S3_BUCKET=${s3_bucket}
EOL

# Clone repository and build Docker image
echo "[$(date)] Cloning repository..."
cd /root
git clone https://github.com/Flor70/bdi.git
cd bdi

# Build and run Docker container with environment variables
echo "[$(date)] Building Docker image..."
docker build -t bdi-api -f docker/Dockerfile .

echo "[$(date)] Starting Docker container..."
docker run -d \
  --env-file /root/.env \
  -p 8080:8080 \
  --name bdi-api \
  bdi-api

# Wait for container to be running
echo "[$(date)] Waiting for container to start..."
sleep 30

# Check container status
echo "[$(date)] Container status:"
docker ps -a

# Create status file when complete
echo "[$(date)] Initialization complete" > /var/log/user-data-status

# Test API
echo "[$(date)] Testing API..."
curl -I http://localhost:8080/docs || echo "API not responding yet"
