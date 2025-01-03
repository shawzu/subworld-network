package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"subworld-network/internal/network"
	"time"
)

func main() {
	isBootstrap := flag.Bool("bootstrap", false, "Run as a bootstrap node")
	bootstrapNode := flag.String("bootstrapNode", "bootstrap-node-address:8080", "Bootstrap node address")
	flag.Parse()

	node, err := network.NewNode(*isBootstrap, *bootstrapNode)
	if err != nil {
		fmt.Println("Failed to create node:", err)
		return
	}

	// Start the node
	go node.Start()

	// If it's a bootstrap node, display its public IP address and port
	if *isBootstrap {
		publicIP := getPublicIP()
		fmt.Printf("Bootstrap node is running on: %s:8080\n", publicIP)
	}

	// Send periodic messages if not a bootstrap node
	if !*isBootstrap {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				message := "Hello, how are you?"
				node.BroadcastMessage(message)
				fmt.Println("Sent message to peers:", message)
			}
		}
	}

	// Keep the program running
	select {}
}

func getPublicIP() string {
	resp, err := http.Get("https://api.ipify.org?format=text")
	if err != nil {
		fmt.Println("Failed to get public IP:", err)
		return "Unknown"
	}
	defer resp.Body.Close()

	ip, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Failed to read response body:", err)
		return "Unknown"
	}

	return string(ip)
}
