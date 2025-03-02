package dht

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"
)

const (
	// K defines the size of k-buckets (maximum number of nodes per bucket)
	K = 20

	// ALPHA defines the number of parallel lookups
	ALPHA = 3

	// IDLength is the length of a node ID in bytes (SHA-1 = 20 bytes)
	IDLength = 20

	// BucketCount is the number of bits in the ID, which determines bucket count
	BucketCount = IDLength * 8

	// RefreshInterval is how often to refresh buckets
	RefreshInterval = 1 * time.Hour

	// ExpireTime is the time after which a node is considered stale
	ExpireTime = 24 * time.Hour
)

// NodeID represents a node's position in the keyspace
type NodeID [IDLength]byte

// String returns a hex representation of the NodeID
func (id NodeID) String() string {
	return hex.EncodeToString(id[:])
}

// FromString creates a NodeID from a hex string
func NodeIDFromString(s string) (NodeID, error) {
	var id NodeID
	bytes, err := hex.DecodeString(s)
	if err != nil {
		return id, err
	}
	if len(bytes) != IDLength {
		return id, fmt.Errorf("invalid ID length: got %d, want %d", len(bytes), IDLength)
	}
	copy(id[:], bytes)
	return id, nil
}

// Equal checks if two NodeIDs are the same
func (id NodeID) Equal(other NodeID) bool {
	return id == other
}

// Distance calculates the XOR distance between two NodeIDs
func (id NodeID) Distance(other NodeID) *big.Int {
	result := big.NewInt(0)

	// Convert each byte to a big.Int
	for i := 0; i < IDLength; i++ {
		// XOR the bytes
		xorByte := id[i] ^ other[i]

		// Shift result left by 8 and OR with xorByte
		result.Lsh(result, 8)
		result.Or(result, big.NewInt(int64(xorByte)))
	}

	return result
}

// PeerInfo represents the information about a node in the DHT
type PeerInfo struct {
	ID        NodeID
	Address   string
	LastSeen  time.Time
	PublicKey string // For secure communication
}

// KBucket represents a k-bucket in the Kademlia routing table
type KBucket struct {
	Nodes       []*PeerInfo
	LastRefresh time.Time
	mutex       sync.RWMutex
}

// DHT represents the Distributed Hash Table
type DHT struct {
	LocalID      NodeID
	LocalAddress string
	Buckets      [BucketCount]*KBucket
	Store        map[string]string // Simple key-value store
	mutex        sync.RWMutex
	RPCTimeout   time.Duration
}

// NewDHT creates a new DHT with the given node info
func NewDHT(address string) *DHT {
	// Generate ID by hashing address
	idHash := sha1.Sum([]byte(address))

	dht := &DHT{
		LocalID:      idHash,
		LocalAddress: address,
		Store:        make(map[string]string),
		RPCTimeout:   5 * time.Second,
	}

	// Initialize buckets
	for i := 0; i < BucketCount; i++ {
		dht.Buckets[i] = &KBucket{
			Nodes:       make([]*PeerInfo, 0, K),
			LastRefresh: time.Now(),
		}
	}

	return dht
}

// FindBucket determines which bucket a node belongs in
func (dht *DHT) FindBucket(id NodeID) int {
	// Calculate distance (XOR) between local node ID and given node ID
	distance := dht.LocalID.Distance(id)

	// Calculate bucket index based on the position of the first set bit
	// When distance is 0 (same ID), we use the last bucket
	if distance.Sign() == 0 {
		return BucketCount - 1
	}

	// Find position of most significant bit
	for i := BucketCount - 1; i >= 0; i-- {
		if distance.Bit(i) == 1 {
			return BucketCount - i - 1
		}
	}

	// Should never happen unless distance calculation is broken
	return 0
}

// AddNode adds a node to the appropriate k-bucket
func (dht *DHT) AddNode(id NodeID, address string, publicKey string) bool {
	// Don't add ourselves
	if id.Equal(dht.LocalID) {
		return false
	}

	bucketIndex := dht.FindBucket(id)
	bucket := dht.Buckets[bucketIndex]

	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	// Check if node is already in bucket
	for i, node := range bucket.Nodes {
		if node.ID.Equal(id) {
			// Update existing node
			bucket.Nodes[i].LastSeen = time.Now()
			bucket.Nodes[i].Address = address
			bucket.Nodes[i].PublicKey = publicKey

			// Move to the end of the list (most recently seen)
			bucket.Nodes = append(bucket.Nodes[:i], bucket.Nodes[i+1:]...)
			bucket.Nodes = append(bucket.Nodes, node)
			return true
		}
	}

	// If bucket isn't full, add the node
	if len(bucket.Nodes) < K {
		bucket.Nodes = append(bucket.Nodes, &PeerInfo{
			ID:        id,
			Address:   address,
			LastSeen:  time.Now(),
			PublicKey: publicKey,
		})
		return true
	}

	// Bucket is full, check if the least recently seen node is stale
	oldestNode := bucket.Nodes[0]
	if time.Since(oldestNode.LastSeen) > ExpireTime {
		// Replace the stale node
		bucket.Nodes = bucket.Nodes[1:]
		bucket.Nodes = append(bucket.Nodes, &PeerInfo{
			ID:        id,
			Address:   address,
			LastSeen:  time.Now(),
			PublicKey: publicKey,
		})
		return true
	}

	// Bucket is full of fresh nodes, can't add
	return false
}

// FindClosestNodes finds the k closest nodes to the given ID
func (dht *DHT) FindClosestNodes(target NodeID, limit int) []*PeerInfo {
	if limit == 0 {
		limit = K
	}

	// Collect all nodes from all buckets
	var allNodes []*PeerInfo

	for i := 0; i < BucketCount; i++ {
		dht.Buckets[i].mutex.RLock()
		allNodes = append(allNodes, dht.Buckets[i].Nodes...)
		dht.Buckets[i].mutex.RUnlock()
	}

	// Calculate distances
	type nodeDistance struct {
		node     *PeerInfo
		distance *big.Int
	}

	distances := make([]nodeDistance, 0, len(allNodes))

	for _, node := range allNodes {
		distance := node.ID.Distance(target)
		distances = append(distances, nodeDistance{node, distance})
	}

	// Sort by distance
	sort.Slice(distances, func(i, j int) bool {
		return distances[i].distance.Cmp(distances[j].distance) < 0
	})

	// Return the k closest
	result := make([]*PeerInfo, 0, limit)
	for i := 0; i < len(distances) && i < limit; i++ {
		result = append(result, distances[i].node)
	}

	return result
}

// StoreValue stores a value in the local key-value store
func (dht *DHT) StoreValue(key, value string) {
	dht.mutex.Lock()
	defer dht.mutex.Unlock()

	dht.Store[key] = value
}

// GetValue retrieves a value from the local key-value store
func (dht *DHT) GetValue(key string) (string, bool) {
	dht.mutex.RLock()
	defer dht.mutex.RUnlock()

	value, exists := dht.Store[key]
	return value, exists
}

// RemoveNode removes a node from the DHT
func (dht *DHT) RemoveNode(id NodeID) {
	bucketIndex := dht.FindBucket(id)
	bucket := dht.Buckets[bucketIndex]

	bucket.mutex.Lock()
	defer bucket.mutex.Unlock()

	for i, node := range bucket.Nodes {
		if node.ID.Equal(id) {
			// Remove the node by index
			bucket.Nodes = append(bucket.Nodes[:i], bucket.Nodes[i+1:]...)
			return
		}
	}
}

// GetPeer returns info about a specific peer if it exists in our routing table
func (dht *DHT) GetPeer(id NodeID) (*PeerInfo, bool) {
	bucketIndex := dht.FindBucket(id)
	bucket := dht.Buckets[bucketIndex]

	bucket.mutex.RLock()
	defer bucket.mutex.RUnlock()

	for _, node := range bucket.Nodes {
		if node.ID.Equal(id) {
			return node, true
		}
	}

	return nil, false
}

// RefreshBucket refreshes a bucket by finding random keys in its range
func (dht *DHT) RefreshBucket(index int) {
	if index >= BucketCount {
		return
	}

	// Mark as refreshed
	dht.Buckets[index].mutex.Lock()
	dht.Buckets[index].LastRefresh = time.Now()
	dht.Buckets[index].mutex.Unlock()

	// In a real implementation, you would generate a random ID in the bucket's range
	// and perform a FindNode operation. This is just a placeholder.
}

// RefreshAllBuckets refreshes all buckets that haven't been refreshed recently
func (dht *DHT) RefreshAllBuckets() {
	for i := 0; i < BucketCount; i++ {
		dht.Buckets[i].mutex.RLock()
		needsRefresh := time.Since(dht.Buckets[i].LastRefresh) > RefreshInterval
		dht.Buckets[i].mutex.RUnlock()

		if needsRefresh {
			dht.RefreshBucket(i)
		}
	}
}

// GetAllPeers returns all peers known to the DHT
func (dht *DHT) GetAllPeers() []*PeerInfo {
	var peers []*PeerInfo

	for i := 0; i < BucketCount; i++ {
		dht.Buckets[i].mutex.RLock()
		peers = append(peers, dht.Buckets[i].Nodes...)
		dht.Buckets[i].mutex.RUnlock()
	}

	return peers
}
