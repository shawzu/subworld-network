package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// AppConfig holds the application configuration
type AppConfig struct {
	// Node settings
	NodeType       string   `json:"node_type"`       // "bootstrap" or "regular"
	BootstrapMode  bool     `json:"bootstrap_mode"`  // Force bootstrap mode
	BootstrapNodes []string `json:"bootstrap_nodes"` // List of bootstrap node addresses
	ListenPort     int      `json:"listen_port"`     // Port to listen for P2P connections

	// API settings
	APIPort        int    `json:"api_port"`         // Port for REST API
	APIBindAddress string `json:"api_bind_address"` // Address to bind API server to

	// Storage settings
	DataDir      string `json:"data_dir"`       // Directory for data storage
	MaxStorageGB int64  `json:"max_storage_gb"` // Maximum storage in GB
	StorageQuota bool   `json:"storage_quota"`  // Enforce storage quota

	// DHT settings
	DHTBucketSize     int `json:"dht_bucket_size"`    // Size of DHT k-buckets
	RefreshInterval   int `json:"refresh_interval"`   // DHT refresh interval in minutes
	ReplicationFactor int `json:"replication_factor"` // Number of nodes to replicate to

	// Security settings
	TLSEnabled  bool   `json:"tls_enabled"`   // Enable TLS for API
	TLSCertFile string `json:"tls_cert_file"` // TLS certificate file
	TLSKeyFile  string `json:"tls_key_file"`  // TLS key file

	// Logging settings
	LogLevel string `json:"log_level"` // Log level (debug, info, warn, error)
	LogFile  string `json:"log_file"`  // Log file path
}

// DefaultConfig creates a default configuration
func DefaultConfig() *AppConfig {
	return &AppConfig{
		NodeType:       "regular",
		BootstrapMode:  false,
		BootstrapNodes: []string{"93.4.27.35:8080"},
		ListenPort:     8080,

		APIPort:        8081,
		APIBindAddress: "0.0.0.0",

		DataDir:      "./data",
		MaxStorageGB: 10,
		StorageQuota: true,

		DHTBucketSize:     20,
		RefreshInterval:   60,
		ReplicationFactor: 10,

		TLSEnabled:  false,
		TLSCertFile: "",
		TLSKeyFile:  "",

		LogLevel: "info",
		LogFile:  "./subworld-network.log",
	}
}

// LoadConfig loads configuration from a file
func LoadConfig(configPath string) (*AppConfig, error) {
	// Start with default config
	config := DefaultConfig()

	// Check if config file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		// No config file, save default
		if err := config.SaveConfig(configPath); err != nil {
			return nil, fmt.Errorf("failed to save default config: %w", err)
		}
		return config, nil
	}

	// Read config file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Parse config
	if err := json.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return config, nil
}

// SaveConfig saves configuration to a file
func (c *AppConfig) SaveConfig(configPath string) error {
	// Create directory if needed
	dir := filepath.Dir(configPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	// Marshal config to JSON
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// Write to file
	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// UpdateFromFlags updates config from command line flags
func (c *AppConfig) UpdateFromFlags(bootstrapMode *bool, dataDir *string,
	apiPort *int, maxStorageGB *int64) {
	if bootstrapMode != nil {
		c.BootstrapMode = *bootstrapMode
		if *bootstrapMode {
			c.NodeType = "bootstrap"
		}
	}

	if dataDir != nil && *dataDir != "" {
		c.DataDir = *dataDir
	}

	if apiPort != nil && *apiPort != 0 {
		c.APIPort = *apiPort
	}

	if maxStorageGB != nil && *maxStorageGB != 0 {
		c.MaxStorageGB = *maxStorageGB
	}
}
