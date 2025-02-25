# FastAPI AWS EC2 Deployment with Terraform

This directory contains Terraform configuration files to deploy the FastAPI application to AWS EC2.

## Prerequisites

1. [Terraform](https://www.terraform.io/downloads.html) installed
2. AWS credentials (already provided in terraform.tfvars)
3. SSH key pair (default: ~/.ssh/id_rsa.pub)

## Deployment Steps

1. Initialize Terraform:
   ```bash
   terraform init
   ```

2. Generate an SSH key pair if you don't have one:
   ```bash
   ssh-keygen -t rsa -b 2048
   ```

3. Plan the deployment:
   ```bash
   terraform plan
   ```

4. Apply the configuration:
   ```bash
   terraform apply
   ```

5. After successful deployment, Terraform will output:
   - The public IP address of the EC2 instance
   - The URL to access the FastAPI application

## Accessing the Application

- FastAPI application: http://<instance_public_ip>:8080
- API documentation: http://<instance_public_ip>:8080/docs

## SSH Access

```bash
ssh ubuntu@<instance_public_ip>
```

## Destroying Resources

When you're done, you can destroy all resources created by Terraform:

```bash
terraform destroy
```

## Notes

- This deployment uses a t2.micro instance, which is part of the AWS Free Tier
- The security group allows traffic on ports 22 (SSH) and 8080 (FastAPI)
- The application is deployed using Docker
