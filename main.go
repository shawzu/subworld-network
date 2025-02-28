package main

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"subworld-network/internal/network"
	"syscall"
)

// Configuration parameters
var (
	// List of bootstrap node IPs (without port)
	bootstrapNodes = []string{
		"93.4.27.35", // Primary bootstrap node
	}
)

func main() {
	fmt.Println("Starting Subworld Network node...")

	// Create a node with our configuration
	config := network.NodeConfig{
		BootstrapNodes: bootstrapNodes,
	}

	node, err := network.NewNode(config)
	if err != nil {
		fmt.Printf("Failed to create node: %v\n", err)
		return
	}

	// Run the node
	err = node.Run()
	if err != nil {
		fmt.Printf("Failed to start node: %v\n", err)
		return
	}

	// Set up message input in a goroutine
	go handleUserInput(node)

	// Set up graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for termination signal
	<-signalChan
	fmt.Println("\nShutting down...")

	// Stop the node
	node.Stop()
}

// handleUserInput reads messages from the console and broadcasts them
func handleUserInput(node *network.Node) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Type messages to broadcast (Ctrl+C to exit):")

	for scanner.Scan() {
		message := scanner.Text()
		if message != "" {
			node.BroadcastMessage(message)
			fmt.Println("Message sent!")
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading from console: %v\n", err)
	}
}
