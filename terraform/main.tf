provider "aws" {
  region     = var.aws_region
  access_key = var.aws_access_key
  secret_key = var.aws_secret_key
  token      = var.aws_session_token
}

# Create a VPC
resource "aws_vpc" "bdi_vpc" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  tags = {
    Name = "bdi-vpc"
  }
}

# Create a subnet
resource "aws_subnet" "bdi_subnet" {
  vpc_id                  = aws_vpc.bdi_vpc.id
  cidr_block              = "10.0.1.0/24"
  map_public_ip_on_launch = true
  availability_zone       = "${var.aws_region}a"
  tags = {
    Name = "bdi-subnet"
  }
}

# Create an internet gateway
resource "aws_internet_gateway" "bdi_igw" {
  vpc_id = aws_vpc.bdi_vpc.id
  tags = {
    Name = "bdi-igw"
  }
}

# Create a route table
resource "aws_route_table" "bdi_rt" {
  vpc_id = aws_vpc.bdi_vpc.id
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.bdi_igw.id
  }
  tags = {
    Name = "bdi-rt"
  }
}

# Associate the route table with the subnet
resource "aws_route_table_association" "bdi_rta" {
  subnet_id      = aws_subnet.bdi_subnet.id
  route_table_id = aws_route_table.bdi_rt.id
}

# Create a security group
resource "aws_security_group" "bdi_sg" {
  name        = "bdi-sg"
  description = "Allow inbound traffic for FastAPI app"
  vpc_id      = aws_vpc.bdi_vpc.id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "SSH"
  }

  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "FastAPI"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "bdi-sg"
  }
}

# Create an EC2 instance
resource "aws_instance" "bdi_instance" {
  ami                    = var.ami_id
  instance_type          = "t2.micro"
  subnet_id              = aws_subnet.bdi_subnet.id
  vpc_security_group_ids = [aws_security_group.bdi_sg.id]
  key_name               = aws_key_pair.bdi_key_pair.key_name

  user_data = <<-EOF
              #!/bin/bash
              apt-get update -y
              apt-get install -y docker.io git
              systemctl start docker
              systemctl enable docker
              
              # Clone the repository
              git clone https://github.com/Flor70/bdi.git /home/ubuntu/bdi
              cd /home/ubuntu/bdi
              
              # Set AWS credentials as environment variables
              echo 'export AWS_ACCESS_KEY_ID=${var.aws_access_key}' >> /home/ubuntu/.bashrc
              echo 'export AWS_SECRET_ACCESS_KEY=${var.aws_secret_key}' >> /home/ubuntu/.bashrc
              echo 'export AWS_SESSION_TOKEN=${var.aws_session_token}' >> /home/ubuntu/.bashrc
              echo 'export AWS_REGION=${var.aws_region}' >> /home/ubuntu/.bashrc
              echo 'export BDI_S3_BUCKET=${var.s3_bucket}' >> /home/ubuntu/.bashrc
              
              # Modify the Dockerfile to fix the directory creation issue
              sed -i 's/chown -R \$USER:\$USER \$APP_DIR/mkdir -p \$APP_DIR \&\& chown -R \$USER:\$USER \$APP_DIR/' docker/Dockerfile
              
              # Build and run the Docker container
              docker build -t bdi-api -f docker/Dockerfile .
              docker run -d --name bdi-api -p 8080:8080 \
                -e AWS_ACCESS_KEY_ID=${var.aws_access_key} \
                -e AWS_SECRET_ACCESS_KEY=${var.aws_secret_key} \
                -e AWS_SESSION_TOKEN=${var.aws_session_token} \
                -e AWS_REGION=${var.aws_region} \
                -e BDI_S3_BUCKET=${var.s3_bucket} \
                bdi-api
              EOF

  tags = {
    Name = "bdi-instance"
  }
}

# Create a key pair for SSH access
resource "aws_key_pair" "bdi_key_pair" {
  key_name   = "bdi-key-pair"
  public_key = file(var.public_key_path)
}

# Output the public IP of the instance
output "instance_public_ip" {
  value = aws_instance.bdi_instance.public_ip
}

output "fastapi_url" {
  value = "http://${aws_instance.bdi_instance.public_ip}:8080"
}
