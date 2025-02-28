# Use the official Go image
FROM golang:1.23

# Set the working directory inside the container
WORKDIR /app

# Copy go.mod and go.sum first to leverage Docker caching
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the rest of the application files
COPY . .

# Build the application
RUN go build -o subworld-network main.go

# Expose the port
EXPOSE 8080

# Command to run the executable
CMD ["./subworld-network"]