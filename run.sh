#!/bin/bash


docker rm -f subworld-node

docker build -t subworld-network .


docker run --name subworld-node -d subworld-network


docker logs -f subworld-node
