#!/bin/bash

# Stop and remove any existing container
docker rm -f subworld-node 2>/dev/null

# Build the docker image
echo "Building Subworld Network container..."
docker build -t subworld-network .

# Run the container with network host mode to ensure proper IP detection
echo "Starting Subworld Network node..."
docker run --name subworld-node -d --network host subworld-network

# Display the logs
echo "Node is running. Displaying logs:"
docker logs -f subworld-node