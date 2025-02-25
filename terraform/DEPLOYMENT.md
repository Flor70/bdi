# FastAPI AWS EC2 Deployment Documentation

## Overview

This document provides information about the deployment of the FastAPI application to AWS EC2 using Terraform.

## Deployment Architecture

The deployment consists of the following AWS resources:

1. **VPC**: A Virtual Private Cloud with CIDR block 10.0.0.0/16
2. **Subnet**: A public subnet with CIDR block 10.0.1.0/24
3. **Internet Gateway**: Allows communication between the VPC and the internet
4. **Route Table**: Routes traffic from the subnet to the internet gateway
5. **Security Group**: Allows inbound traffic on ports 22 (SSH) and 8080 (FastAPI)
6. **EC2 Instance**: A t2.micro instance running Ubuntu 22.04 LTS
7. **Key Pair**: An SSH key pair for secure access to the EC2 instance

## Application Setup

The application is deployed using Docker. The deployment process includes:

1. Cloning the repository from GitHub
2. Setting up AWS credentials as environment variables
3. Building the Docker image using the Dockerfile
4. Running the Docker container with the necessary environment variables

## Access Information

- **FastAPI Application**: http://<instance_public_ip>:8080
- **API Documentation**: http://<instance_public_ip>:8080/docs
- **SSH Access**: `ssh ubuntu@<instance_public_ip>`

## AWS Credentials

The deployment uses the following AWS credentials:

- **AWS Access Key**: ASIAWZKXMWET227CIJTZ
- **AWS Region**: us-east-1
- **S3 Bucket**: bdi-aircraft-floriano

## Maintenance

### Updating the Application

To update the application, SSH into the EC2 instance and pull the latest changes from GitHub:

```bash
ssh ubuntu@<instance_public_ip>
cd /home/ubuntu/bdi
git pull
docker stop bdi-api
docker rm bdi-api
docker build -t bdi-api -f docker/Dockerfile .
docker run -d --name bdi-api -p 8080:8080 \
  -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
  -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
  -e AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN} \
  -e AWS_REGION=${AWS_REGION} \
  -e BDI_S3_BUCKET=${BDI_S3_BUCKET} \
  bdi-api
```

### Checking Container Logs

To check the logs of the Docker container:

```bash
ssh ubuntu@<instance_public_ip>
docker logs bdi-api
```

## Cleanup

To destroy all resources created by Terraform:

```bash
cd terraform
terraform destroy
```

## Security Considerations

This deployment is for educational purposes only and has minimal security measures:

- The security group allows inbound traffic from any IP address
- AWS credentials are stored in environment variables
- No HTTPS is configured

For a production environment, additional security measures would be required, such as:

- Restricting inbound traffic to specific IP addresses
- Using AWS Secrets Manager for credential management
- Configuring HTTPS with a valid SSL certificate
- Implementing proper authentication and authorization
