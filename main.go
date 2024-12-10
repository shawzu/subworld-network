package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"subworld-network/internal/cryptography"
	"subworld-network/internal/network"
	"syscall"
)

func main() {
	fmt.Println("Subworld Network starting...")

	// Initialize cryptographic keys
	privateKey, publicKey := cryptography.GenerateKeyPair()
	fmt.Println("Generated Private Key:", privateKey)
	fmt.Println("Generated Public Key:", publicKey)

	// Start the network node
	if err := network.StartNode(); err != nil {
		log.Fatalf("Failed to start network: %v", err)
	}

	// Wait for shutdown signals (e.g., CTRL+C)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	fmt.Println("Subworld Network is running. Press CTRL+C to stop.")

	<-stop // Block until a signal is received

	fmt.Println("Shutting down Subworld Network...")
	// Perform any necessary cleanup here (e.g., closing connections)
}
