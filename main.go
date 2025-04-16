package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"subworld-network/internal/api"
	"subworld-network/internal/maintenance"
	"subworld-network/internal/network"
	"subworld-network/internal/storage"
	"syscall"
	"time"
)

func main() {
	// Parse command line flags
	rand.Seed(time.Now().UnixNano())
	isBootstrap := flag.Bool("bootstrap", false, "Force this node to run as a bootstrap node")
	dataDir := flag.String("datadir", "./data", "Data directory for storage")
	apiPort := flag.Int("apiport", 8081, "Port for the API server")
	apiBindAddr := flag.String("apibind", "0.0.0.0", "API bind address")
	p2pPort := flag.Int("p2pport", 8080, "Port for P2P network")
	maxStorageGB := flag.Int64("storage", 10, "Maximum storage in GB")
	enableTLS := flag.Bool("tls", false, "Enable TLS for API")
	tlsCert := flag.String("tlscert", "", "TLS certificate file")
	tlsKey := flag.String("tlskey", "", "TLS key file")
	logDir := flag.String("logdir", "./logs", "Directory for log files")
	flag.Parse()

	fmt.Println("Starting Subworld Network node...")

	// Bootstrap node configuration
	bootstrapNodes := []string{
		"93.4.27.35", // Primary bootstrap node
		"167.71.11.170",
		"178.62.199.31",
	}

	// Create node configuration
	config := network.NodeConfig{
		BootstrapNodes: bootstrapNodes,
		ForceBootstrap: *isBootstrap,
		ListenPort:     *p2pPort,
		MaxPeers:       100, // Default: allow up to 100 peers
	}

	// Initialize storage
	nodeStorage, err := storage.NewNodeStorage(*dataDir, *maxStorageGB)
	if err != nil {
		fmt.Printf("Failed to initialize storage: %v\n", err)
		return
	}
	defer nodeStorage.Close()

	// Create and run the node
	node, err := network.NewNode(config)
	if err != nil {
		fmt.Printf("Failed to create node: %v\n", err)
		return
	}

	// Set storage for the node
	node.SetStorage(nodeStorage)

	// Start the node
	err = node.Run()
	if err != nil {
		fmt.Printf("Failed to start node: %v\n", err)
		return
	}

	// Create and start maintenance manager
	maintenanceConfig := maintenance.DefaultMaintenanceConfig()
	maintenanceConfig.LogDir = *logDir
	maintenanceMgr := maintenance.NewMaintenanceManager(node, nodeStorage, maintenanceConfig)
	maintenanceMgr.Start()

	// Start API server
	apiConfig := api.APIConfig{
		BindAddress:     *apiBindAddr,
		Port:            *apiPort,
		TLSEnabled:      *enableTLS,
		TLSCertFile:     *tlsCert,
		TLSKeyFile:      *tlsKey,
		MaxUploadSizeMB: 10, // Default to 10MB max upload
	}

	apiServer := api.NewNodeAPI(node, nodeStorage, apiConfig)
	go func() {
		if err := apiServer.StartServer(); err != nil {
			fmt.Printf("API server error: %v\n", err)
		}
	}()

	fmt.Printf("Node started as %s, API server running on %s:%d\n",
		node.GetNodeType(), *apiBindAddr, *apiPort)

	// Display connection info
	fmt.Printf("Node address: %s\n", node.GetAddress())
	if node.GetNodeType() == network.BootstrapNode {
		fmt.Println("Operating as a bootstrap node")
	} else {
		fmt.Println("Operating as a regular node")
		fmt.Println("Connected to bootstrap nodes: ", bootstrapNodes)
	}

	// Print API endpoint for clients
	if *enableTLS {
		fmt.Printf("API endpoint for clients: https://%s:%d\n", *apiBindAddr, *apiPort)
	} else {
		fmt.Printf("API endpoint for clients: http://%s:%d\n", *apiBindAddr, *apiPort)
	}

	fmt.Println("Press Ctrl+C to exit")

	// Set up graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for termination signal
	<-signalChan
	fmt.Println("\nShutting down...")

	// Stop maintenance
	maintenanceMgr.Stop()

	// Allow time for graceful shutdown
	fmt.Println("Waiting for tasks to complete...")
	time.Sleep(2 * time.Second)

	// Stop the node
	node.Stop()

	fmt.Println("Node stopped successfully")
}
