#!/bin/bash

# Get the instance public IP
INSTANCE_IP=$(terraform output -raw instance_public_ip)
API_URL="http://${INSTANCE_IP}:8080"

echo "Checking FastAPI application at: ${API_URL}"
echo "This may take a few minutes as the EC2 instance initializes..."

# Try to connect to the API
for i in {1..30}; do
  echo "Attempt $i: Checking if the API is up..."
  if curl -s --head --request GET "${API_URL}" | grep "200 OK" > /dev/null; then
    echo "Success! The FastAPI application is running."
    echo "You can access the API at: ${API_URL}"
    echo "API documentation is available at: ${API_URL}/docs"
    exit 0
  fi
  
  echo "API not ready yet. Waiting 10 seconds before trying again..."
  sleep 10
done

echo "Could not connect to the API after multiple attempts."
echo "The instance might still be initializing or there might be an issue with the deployment."
echo "You can try accessing the API manually at: ${API_URL}"
echo "Or SSH into the instance to check the status: ssh ubuntu@${INSTANCE_IP}"
