#!/bin/bash

echo "Building Subworld Network..."
go build -o subworld-network main.go

echo "Starting Subworld Network in REGULAR MODE..."
./subworld-network