#!/bin/bash

# Instalar Docker
apt-get update
apt-get install -y apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -
add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io git

# Clonar repositório
git clone https://github.com/Flor70/bdi.git /opt/bdi
cd /opt/bdi

# Construir e executar Docker
docker build -t bdi-api -f docker/Dockerfile .
docker run -d \
  --name bdi-api \
  -p 8080:8080 \
  -e BDI_S3_BUCKET=${s3_bucket} \
  -e AWS_ACCESS_KEY_ID=${aws_access_key} \
  -e AWS_SECRET_ACCESS_KEY=${aws_secret_key} \
  -e AWS_SESSION_TOKEN=${aws_session_token} \
  -e AWS_DEFAULT_REGION=${aws_region} \
  --restart always \
  bdi-api
