package network

import (
	"fmt"
	"time"
)

// StartNode simulates a running network node.
func StartNode() error {
	fmt.Println("Network node started successfully")

	// Simulate node activity
	go func() {
		for {
			fmt.Println("Node is running...")
			time.Sleep(5 * time.Second) // Simulate work
		}
	}()

	return nil
}
