package maintenance

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"subworld-network/internal/network"
	"subworld-network/internal/storage"
	"sync"
	"time"
)

// MaintenanceConfig contains configuration for maintenance tasks
type MaintenanceConfig struct {
	ExpiredContentInterval time.Duration // How often to clean up expired content
	DHTRefreshInterval     time.Duration // How often to refresh DHT buckets
	StorageQuotaInterval   time.Duration // How often to check storage quotas
	LogRotationInterval    time.Duration // How often to rotate logs
	MaxLogSizeMB           int64         // Maximum log file size in MB before rotation
	MaxLogFiles            int           // Maximum number of log files to keep
	LogDir                 string        // Directory for log files
}

// DefaultMaintenanceConfig returns default configuration values
func DefaultMaintenanceConfig() MaintenanceConfig {
	return MaintenanceConfig{
		ExpiredContentInterval: 5 * time.Minute,
		DHTRefreshInterval:     1 * time.Hour,
		StorageQuotaInterval:   6 * time.Hour,
		LogRotationInterval:    24 * time.Hour,
		MaxLogSizeMB:           100,
		MaxLogFiles:            5,
		LogDir:                 "./logs",
	}
}

// MaintenanceManager handles periodic maintenance tasks
type MaintenanceManager struct {
	node            *network.Node
	storage         *storage.NodeStorage
	config          MaintenanceConfig
	running         bool
	stopChan        chan struct{}
	wg              sync.WaitGroup
	lastMemoryStats time.Time
}

// NewMaintenanceManager creates a new maintenance manager
func NewMaintenanceManager(node *network.Node, storage *storage.NodeStorage, config MaintenanceConfig) *MaintenanceManager {
	// Ensure log directory exists
	if config.LogDir != "" {
		os.MkdirAll(config.LogDir, 0755)
	}

	return &MaintenanceManager{
		node:            node,
		storage:         storage,
		config:          config,
		stopChan:        make(chan struct{}),
		lastMemoryStats: time.Now(),
	}
}

// Start begins running maintenance tasks
func (m *MaintenanceManager) Start() {
	if m.running {
		fmt.Println("Maintenance manager is already running")
		return
	}

	m.running = true

	// Start the maintenance goroutines
	m.wg.Add(4)
	go m.runExpiredContentCleanup()
	go m.runDHTRefresh()
	go m.runStorageQuotaCheck()
	go m.runLogRotation()

	// Additional diagnostic task
	m.wg.Add(1)
	go m.runResourceMonitoring()

	fmt.Println("Maintenance manager started")
}

// Stop stops maintenance tasks
func (m *MaintenanceManager) Stop() {
	if !m.running {
		return
	}

	m.running = false
	close(m.stopChan)
	m.wg.Wait()

	fmt.Println("Maintenance manager stopped")
}

// runExpiredContentCleanup periodically cleans up expired content
func (m *MaintenanceManager) runExpiredContentCleanup() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.ExpiredContentInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fmt.Println("Running expired content cleanup...")
			start := time.Now()

			if err := m.storage.CleanupExpiredContent(); err != nil {
				fmt.Printf("Error cleaning up expired content: %v\n", err)
			}

			fmt.Printf("Expired content cleanup completed in %s\n", time.Since(start))

		case <-m.stopChan:
			return
		}
	}
}

// runDHTRefresh periodically refreshes DHT buckets
func (m *MaintenanceManager) runDHTRefresh() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.DHTRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fmt.Println("Running DHT refresh...")
			start := time.Now()

			m.node.RefreshDHT()

			fmt.Printf("DHT refresh completed in %s\n", time.Since(start))

		case <-m.stopChan:
			return
		}
	}
}

// runStorageQuotaCheck checks storage usage
func (m *MaintenanceManager) runStorageQuotaCheck() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.StorageQuotaInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fmt.Println("Running storage quota check...")
			start := time.Now()

			// Check data directory size
			if m.storage != nil {
				dataDir := m.storage.GetDataDir()
				if dataDir != "" {
					size, err := getDirSize(dataDir)
					if err != nil {
						fmt.Printf("Error checking data directory size: %v\n", err)
					} else {
						// Convert to GB
						sizeGB := float64(size) / (1024 * 1024 * 1024)
						fmt.Printf("Current storage usage: %.2f GB\n", sizeGB)

						// Check against maximum
						maxStorageGB := float64(m.storage.GetMaxStorageGB())
						if sizeGB > maxStorageGB*0.9 {
							fmt.Printf("WARNING: Storage usage (%.2f GB) is approaching limit (%.2f GB)\n",
								sizeGB, maxStorageGB)

							// In a production system, you might want to implement automated cleanup here
							// Example: Delete oldest messages that have been marked as delivered
						}
					}
				}
			}

			fmt.Printf("Storage quota check completed in %s\n", time.Since(start))

		case <-m.stopChan:
			return
		}
	}
}

// runLogRotation handles log file rotation
func (m *MaintenanceManager) runLogRotation() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.config.LogRotationInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			fmt.Println("Running log rotation...")
			start := time.Now()

			if m.config.LogDir != "" {
				// Check for log files to rotate
				logFiles, err := filepath.Glob(filepath.Join(m.config.LogDir, "*.log"))
				if err != nil {
					fmt.Printf("Error finding log files: %v\n", err)
					continue
				}

				for _, logFile := range logFiles {
					info, err := os.Stat(logFile)
					if err != nil {
						fmt.Printf("Error stat'ing log file %s: %v\n", logFile, err)
						continue
					}

					// Check if file exceeds max size
					if info.Size() > m.config.MaxLogSizeMB*1024*1024 {
						// Rotate the file
						rotateLogFile(logFile, m.config.MaxLogFiles)
					}
				}
			}

			fmt.Printf("Log rotation completed in %s\n", time.Since(start))

		case <-m.stopChan:
			return
		}
	}
}

// rotateLogFile rotates a log file, keeping a maximum number of old files
func rotateLogFile(logFile string, maxFiles int) error {
	// Delete the oldest log file if we already have maxFiles
	oldestLog := fmt.Sprintf("%s.%d", logFile, maxFiles-1)
	if _, err := os.Stat(oldestLog); err == nil {
		os.Remove(oldestLog)
	}

	// Shift all existing log files
	for i := maxFiles - 2; i >= 0; i-- {
		oldFile := logFile
		if i > 0 {
			oldFile = fmt.Sprintf("%s.%d", logFile, i)
		}

		newFile := fmt.Sprintf("%s.%d", logFile, i+1)

		if _, err := os.Stat(oldFile); err == nil {
			os.Rename(oldFile, newFile)
		}
	}

	// Create a new empty log file
	f, err := os.Create(logFile)
	if err != nil {
		return err
	}
	f.Close()

	return nil
}

// runResourceMonitoring periodically reports resource usage
func (m *MaintenanceManager) runResourceMonitoring() {
	defer m.wg.Done()

	// Check every minute
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Only log memory stats every 10 minutes to avoid log spam
			if time.Since(m.lastMemoryStats) >= 10*time.Minute {
				m.logMemoryStats()
				m.lastMemoryStats = time.Now()
			}

		case <-m.stopChan:
			return
		}
	}
}

// logMemoryStats logs information about memory usage
func (m *MaintenanceManager) logMemoryStats() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	fmt.Printf("Memory stats - Alloc: %v MB, Sys: %v MB, NumGC: %v\n",
		memStats.Alloc/(1024*1024),
		memStats.Sys/(1024*1024),
		memStats.NumGC)

	// In a production system, you might want to store these metrics
	// or send them to a monitoring system
}

// getDirSize calculates the total size of a directory and its contents
func getDirSize(path string) (int64, error) {
	var size int64

	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})

	return size, err
}

// PerformManualCleanup runs an immediate cleanup operation
// This can be triggered externally when needed
func (m *MaintenanceManager) PerformManualCleanup() error {
	fmt.Println("Performing manual cleanup...")
	start := time.Now()

	// Cleanup expired content
	if err := m.storage.CleanupExpiredContent(); err != nil {
		return fmt.Errorf("error cleaning up expired content: %w", err)
	}

	// Could add additional cleanup tasks here

	fmt.Printf("Manual cleanup completed in %s\n", time.Since(start))
	return nil
}

// GetStatusReport returns information about maintenance tasks
func (m *MaintenanceManager) GetStatusReport() map[string]interface{} {
	report := map[string]interface{}{
		"running":                  m.running,
		"expired_content_interval": m.config.ExpiredContentInterval.String(),
		"dht_refresh_interval":     m.config.DHTRefreshInterval.String(),
		"storage_quota_interval":   m.config.StorageQuotaInterval.String(),
		"log_rotation_interval":    m.config.LogRotationInterval.String(),
		"last_memory_stats_check":  m.lastMemoryStats.Format(time.RFC3339),
	}

	// Add storage info if available
	if m.storage != nil {
		dataDir := m.storage.GetDataDir()
		if dataDir != "" {
			size, err := getDirSize(dataDir)
			if err == nil {
				report["storage_size_bytes"] = size
				report["storage_size_mb"] = float64(size) / (1024 * 1024)
				report["storage_size_gb"] = float64(size) / (1024 * 1024 * 1024)
				report["max_storage_gb"] = m.storage.GetMaxStorageGB()
			}
		}
	}

	return report
}
