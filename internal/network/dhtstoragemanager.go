package network

import (
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"subworld-network/internal/dht"
	"subworld-network/internal/storage"
	"time"
)

const (
	// ReplicationFactor defines how many nodes should store each piece of content
	ReplicationFactor = 10
)

// DHTStorageManager handles content distribution across the network
type DHTStorageManager struct {
	node    *Node
	storage *storage.NodeStorage
}

type PhotoUploadResult struct {
	ID         string `json:"id"`
	ChunkIndex int    `json:"chunk_index"`
	Status     string `json:"status"`
	TotalSize  int    `json:"total_size,omitempty"`
}

// NewDHTStorageManager creates a new storage manager
func NewDHTStorageManager(node *Node, storage *storage.NodeStorage) *DHTStorageManager {
	return &DHTStorageManager{
		node:    node,
		storage: storage,
	}
}

// StoreContent stores content locally and replicates it to other nodes
func (m *DHTStorageManager) StoreContent(content *storage.EncryptedContent) error {
	// Store locally first
	if err := m.storage.StoreContent(content); err != nil {
		return err
	}

	// Don't replicate call signals - they're short-lived
	if content.Type == storage.TypeCallSignal {
		return nil
	}

	// Determine which nodes should store this content
	contentKey := fmt.Sprintf("content:%s:%s", content.RecipientID, content.ID)
	contentHash := HashString(contentKey)
	targetID := dht.NodeID(contentHash)

	// Find the closest nodes to this content key
	closestNodes := m.node.dht.FindClosestNodes(targetID, ReplicationFactor)

	// Serialize the content
	contentData, err := json.Marshal(content)
	if err != nil {
		return fmt.Errorf("failed to marshal content: %w", err)
	}

	// Create store message
	storeMsg := Message{
		Type:    CmdStoreContent,
		Sender:  m.node.address,
		Content: string(contentData),
	}
	msgData, _ := json.Marshal(storeMsg)

	// Replicate to target nodes
	for _, targetNode := range closestNodes {
		// Skip if it's ourselves - we already stored it
		if targetNode.ID.Equal(m.node.dht.LocalID) {
			continue
		}

		// Send to node
		if conn, ok := m.node.getPeerByAddress(targetNode.Address); ok {
			conn.Write(msgData)
		} else {
			// Try to establish connection if not connected
			if conn, err := net.Dial("tcp", targetNode.Address); err == nil {
				// Send the store message
				conn.Write(msgData)

				// Add to peers and handle future messages
				m.node.peersMutex.Lock()
				m.node.peers[targetNode.Address] = conn
				m.node.peersMutex.Unlock()

				go m.node.handlePeer(conn)
			}
		}
	}

	return nil
}

// LookupContent tries to find content across the network
func (m *DHTStorageManager) LookupContent(recipientID, contentID string) (*storage.EncryptedContent, error) {
	// Try local storage first
	messages, err := m.storage.GetMessagesByUser(recipientID, false)
	if err == nil {
		for _, msg := range messages {
			if msg.ID == contentID {
				return msg, nil
			}
		}
	}

	// Not found locally, search the network
	contentKey := fmt.Sprintf("content:%s:%s", recipientID, contentID)
	targetID := dht.NodeID(HashString(contentKey))

	// Find nodes that might have this content
	closestNodes := m.node.dht.FindClosestNodes(targetID, ReplicationFactor*2)

	// Create find content request
	findReq := Message{
		Type:    CmdFindContent,
		Sender:  m.node.address,
		Content: fmt.Sprintf("%s:%s", recipientID, contentID),
	}

	// Keep track of which nodes we've queried
	queriedNodes := make(map[string]bool)

	// In a real implementation, you'd use a more sophisticated approach with channels
	// and timeouts. This is simplified for clarity.

	// Query each node
	for _, node := range closestNodes {
		// Skip if it's ourselves - we already checked
		if node.ID.Equal(m.node.dht.LocalID) || queriedNodes[node.Address] {
			continue
		}

		queriedNodes[node.Address] = true

		// Send request
		if conn, ok := m.node.getPeerByAddress(node.Address); ok {
			reqData, _ := json.Marshal(findReq)
			conn.Write(reqData)
		} else {
			// Try to establish connection if not connected
			if conn, err := net.Dial("tcp", node.Address); err == nil {
				reqData, _ := json.Marshal(findReq)
				conn.Write(reqData)

				// Add to peers and handle future messages
				m.node.peersMutex.Lock()
				m.node.peers[node.Address] = conn
				m.node.peersMutex.Unlock()

				go m.node.handlePeer(conn)
			}
		}
	}

	// In a real implementation, you'd wait for responses here
	// For simplicity, we just return not found
	return nil, fmt.Errorf("content not found in network")
}

// FetchAllUserContent attempts to fetch all content for a user from the network
func (m *DHTStorageManager) FetchAllUserContent(userID string) []*storage.EncryptedContent {

	// Create a lookup key for this user's content
	userKey := fmt.Sprintf("user:%s:content", userID)
	targetID := dht.NodeID(HashString(userKey))

	// Find nodes that might have content for this user
	closestNodes := m.node.dht.FindClosestNodes(targetID, ReplicationFactor*2)

	// Create content request message
	contentReq := Message{
		Type:    CmdFindAllUserContent,
		Sender:  m.node.address,
		Content: userID,
	}
	reqData, _ := json.Marshal(contentReq)

	// Keep track of which nodes we've queried
	queriedNodes := make(map[string]bool)

	// Query each node
	for _, node := range closestNodes {
		// Skip if it's ourselves or already queried
		if node.ID.Equal(m.node.dht.LocalID) || queriedNodes[node.Address] {
			continue
		}

		queriedNodes[node.Address] = true

		// Send request
		if conn, ok := m.node.getPeerByAddress(node.Address); ok {
			conn.Write(reqData)
		} else {
			// Try to establish connection if not connected
			if conn, err := net.Dial("tcp", node.Address); err == nil {
				conn.Write(reqData)

				// Add to peers and handle future messages
				m.node.peersMutex.Lock()
				m.node.peers[node.Address] = conn
				m.node.peersMutex.Unlock()

				go m.node.handlePeer(conn)
			}
		}
	}

	// Wait a short time for responses (would be better with proper async handling)
	time.Sleep(500 * time.Millisecond)

	// Fetch local content again (might have been updated by responses)
	updatedContent, _ := m.storage.GetMessagesByUser(userID, false)

	return updatedContent
}

func (m *DHTStorageManager) RegisterUser(username, userInfo string) error {
	// Create key for the user
	userKey := fmt.Sprintf("user:%s", username)

	// Store locally in DHT
	m.node.dht.StoreValue(userKey, userInfo)

	// Find nodes that should store this user info
	userID := dht.NodeID(HashString(userKey))
	targetNodes := m.node.dht.FindClosestNodes(userID, ReplicationFactor)

	// Create store message
	contentObj := map[string]interface{}{
		"key":   userKey,
		"value": json.RawMessage(userInfo), // This treats userInfo as raw JSON
		"type":  "user_info",
	}
	contentBytes, err := json.Marshal(contentObj)
	if err != nil {
		return fmt.Errorf("failed to marshal user info: %w", err)
	}

	storeMsg := Message{
		Type:    CmdStoreContent,
		Sender:  m.node.address,
		Content: string(contentBytes),
	}

	msgData, _ := json.Marshal(storeMsg)

	// Distribute to target nodes
	for _, node := range targetNodes {
		// Skip ourselves
		if node.ID.Equal(m.node.dht.LocalID) {
			continue
		}

		// Send to node
		if conn, ok := m.node.getPeerByAddress(node.Address); ok {
			conn.Write(msgData)
		} else {
			// Try to establish connection
			if conn, err := net.Dial("tcp", node.Address); err == nil {
				conn.Write(msgData)
				m.node.peersMutex.Lock()
				m.node.peers[node.Address] = conn
				m.node.peersMutex.Unlock()
				go m.node.handlePeer(conn)
			}
		}
	}

	return nil
}

// StoreFile stores a file chunk and distributes it across the network
func (m *DHTStorageManager) StoreFile(file *storage.EncryptedContent) error {
	// Store locally first
	if err := m.storage.StoreContent(file); err != nil {
		return err
	}

	// Determine which nodes should store this file
	fileKey := fmt.Sprintf("file:%s:%s:%d", file.RecipientID, file.ID, file.ChunkIndex)
	fileHash := HashString(fileKey)
	targetID := dht.NodeID(fileHash)

	// Find closest nodes
	closestNodes := m.node.dht.FindClosestNodes(targetID, ReplicationFactor)

	// Serialize the file
	fileData, err := json.Marshal(file)
	if err != nil {
		return fmt.Errorf("failed to marshal file: %w", err)
	}

	// Create store message
	storeMsg := Message{
		Type:    CmdStoreContent,
		Sender:  m.node.address,
		Content: string(fileData),
	}
	msgData, _ := json.Marshal(storeMsg)

	// Distribute to target nodes
	for _, node := range closestNodes {
		if node.ID.Equal(m.node.dht.LocalID) {
			continue // Skip ourselves
		}

		// Send to node
		if conn, ok := m.node.getPeerByAddress(node.Address); ok {
			conn.Write(msgData)
		} else {
			// Try to establish connection
			if conn, err := net.Dial("tcp", node.Address); err == nil {
				conn.Write(msgData)
				m.node.peersMutex.Lock()
				m.node.peers[node.Address] = conn
				m.node.peersMutex.Unlock()
				go m.node.handlePeer(conn)
			}
		}
	}

	return nil
}

// FetchFile tries to find a file across the network
func (m *DHTStorageManager) FetchFile(userID, fileID string, chunkIndex int) (*storage.EncryptedContent, error) {
	files, err := m.storage.GetContentByType(userID, storage.TypeFile)
	if err == nil {
		for _, file := range files {
			if file.ID == fileID && file.ChunkIndex == chunkIndex {
				// Ensure we have data in both fields
				if len(file.RawData) == 0 && file.EncryptedData != "" {
					file.RawData = []byte(file.EncryptedData)
				}
				return file, nil
			}
		}
	}

	// Not found locally, search the network
	fileKey := fmt.Sprintf("file:%s:%s:%d", userID, fileID, chunkIndex)
	targetID := dht.NodeID(HashString(fileKey))

	// Find nodes that might have this file
	closestNodes := m.node.dht.FindClosestNodes(targetID, ReplicationFactor*2)

	// Create find file request
	findReq := Message{
		Type:    CmdFindFile,
		Sender:  m.node.address,
		Content: fmt.Sprintf("%s:%s:%d", userID, fileID, chunkIndex),
	}
	reqData, _ := json.Marshal(findReq)

	// Track queried nodes
	queriedNodes := make(map[string]bool)

	// Set up channel for results
	resultChan := make(chan *storage.EncryptedContent, 1)

	// Query each node
	for _, node := range closestNodes {
		if node.ID.Equal(m.node.dht.LocalID) || queriedNodes[node.Address] {
			continue
		}

		queriedNodes[node.Address] = true

		go func(nodeAddr string) {
			if conn, ok := m.node.getPeerByAddress(nodeAddr); ok {
				conn.Write(reqData)
			} else {
				if conn, err := net.Dial("tcp", nodeAddr); err == nil {
					conn.Write(reqData)
					m.node.peersMutex.Lock()
					m.node.peers[nodeAddr] = conn
					m.node.peersMutex.Unlock()
					go m.node.handlePeer(conn)
				}
			}
		}(node.Address)
	}

	// Wait for result or timeout
	select {
	case result := <-resultChan:
		// Store locally for future queries
		m.storage.StoreContent(result)
		return result, nil
	case <-time.After(2 * time.Second):
		return nil, fmt.Errorf("file not found in network")
	}
}

// StoreVoiceStream stores voice stream chunks to the DHT
func (m *DHTStorageManager) StoreVoiceStream(content *storage.EncryptedContent) error {
	// Store locally first
	if err := m.storage.StoreContent(content); err != nil {
		return err
	}

	// Determine which nodes should store this content
	contentKey := fmt.Sprintf("voice:%s:%s", content.RecipientID, content.ID)
	contentHash := HashString(contentKey)
	targetID := dht.NodeID(contentHash)

	// Find the closest nodes to this content key
	closestNodes := m.node.dht.FindClosestNodes(targetID, ReplicationFactor)

	// Serialize the content
	contentData, err := json.Marshal(content)
	if err != nil {
		return fmt.Errorf("failed to marshal voice content: %w", err)
	}

	// Create store message
	storeMsg := Message{
		Type:    CmdStoreContent,
		Sender:  m.node.address,
		Content: string(contentData),
	}
	msgData, _ := json.Marshal(storeMsg)

	// Replicate to target nodes - prioritize fast delivery for voice
	for _, targetNode := range closestNodes {
		// Skip if it's ourselves - we already stored it
		if targetNode.ID.Equal(m.node.dht.LocalID) {
			continue
		}

		// Send to node
		if conn, ok := m.node.getPeerByAddress(targetNode.Address); ok {
			// Set a short deadline for voice data to ensure fresh data
			conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			conn.Write(msgData)
			conn.SetWriteDeadline(time.Time{}) // Reset deadline
		} else {
			// Try to establish connection if not connected
			if conn, err := net.Dial("tcp", targetNode.Address); err == nil {
				conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
				conn.Write(msgData)
				conn.SetWriteDeadline(time.Time{}) // Reset deadline

				// Add to peers and handle future messages
				m.node.peersMutex.Lock()
				m.node.peers[targetNode.Address] = conn
				m.node.peersMutex.Unlock()

				go m.node.handlePeer(conn)
			}
		}
	}

	return nil
}

// FetchVoiceStream fetches voice stream chunks for a recipient
func (m *DHTStorageManager) FetchVoiceStream(recipientID, callSessionID string, since time.Time) ([]*storage.EncryptedContent, error) {
	// Try local storage first
	chunks, err := m.storage.GetContentByType(recipientID, storage.TypeVoiceStream)

	// Filter by call session if provided
	if callSessionID != "" && err == nil {
		var filteredChunks []*storage.EncryptedContent
		for _, chunk := range chunks {
			if strings.HasPrefix(chunk.ID, callSessionID) || chunk.ID == callSessionID {
				filteredChunks = append(filteredChunks, chunk)
			}
		}
		chunks = filteredChunks
	}

	// Filter by timestamp
	if !since.IsZero() && err == nil {
		var filteredChunks []*storage.EncryptedContent
		for _, chunk := range chunks {
			if chunk.Timestamp.After(since) {
				filteredChunks = append(filteredChunks, chunk)
			}
		}
		chunks = filteredChunks
	}

	// Build lookup key for content in the DHT
	voiceKey := fmt.Sprintf("voice:%s", recipientID)
	targetID := dht.NodeID(HashString(voiceKey))

	// Find nodes that might have voice data for this user
	closestNodes := m.node.dht.FindClosestNodes(targetID, ReplicationFactor*2)

	// Create request message
	findReq := Message{
		Type:    CmdFindVoiceStream,
		Sender:  m.node.address,
		Content: fmt.Sprintf("%s:%s", recipientID, callSessionID),
	}
	reqData, _ := json.Marshal(findReq)

	// Keep track of which nodes we've queried
	queriedNodes := make(map[string]bool)

	// Query each node
	for _, node := range closestNodes {
		// Skip if it's ourselves or already queried
		if node.ID.Equal(m.node.dht.LocalID) || queriedNodes[node.Address] {
			continue
		}

		queriedNodes[node.Address] = true

		// Send request with short timeout for voice data
		if conn, ok := m.node.getPeerByAddress(node.Address); ok {
			conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
			conn.Write(reqData)
			conn.SetWriteDeadline(time.Time{}) // Reset deadline
		} else {
			// Try to establish connection if not connected
			if conn, err := net.Dial("tcp", node.Address); err == nil {
				conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
				conn.Write(reqData)
				conn.SetWriteDeadline(time.Time{}) // Reset deadline

				// Add to peers and handle future messages
				m.node.peersMutex.Lock()
				m.node.peers[node.Address] = conn
				m.node.peersMutex.Unlock()

				go m.node.handlePeer(conn)
			}
		}
	}

	// Short wait for responses to arrive
	time.Sleep(500 * time.Millisecond)

	// Get updated content (which may have been updated by responses)
	updatedChunks, _ := m.storage.GetContentByType(recipientID, storage.TypeVoiceStream)

	// Re-filter with the same criteria
	if callSessionID != "" {
		var filteredChunks []*storage.EncryptedContent
		for _, chunk := range updatedChunks {
			if strings.HasPrefix(chunk.ID, callSessionID) || chunk.ID == callSessionID {
				filteredChunks = append(filteredChunks, chunk)
			}
		}
		updatedChunks = filteredChunks
	}

	if !since.IsZero() {
		var filteredChunks []*storage.EncryptedContent
		for _, chunk := range updatedChunks {
			if chunk.Timestamp.After(since) {
				filteredChunks = append(filteredChunks, chunk)
			}
		}
		updatedChunks = filteredChunks
	}

	return updatedChunks, nil
}

// FindUser looks up a user across the network
func (m *DHTStorageManager) FindUser(username string) (string, bool) {
	// Try local DHT first
	userKey := fmt.Sprintf("user:%s", username)
	userInfo, found := m.node.dht.GetValue(userKey)
	if found {
		return userInfo, true
	}

	// Not found locally, search the network
	userID := dht.NodeID(HashString(userKey))
	closestNodes := m.node.dht.FindClosestNodes(userID, ReplicationFactor*2)

	// Create channel for results and register it
	resultChan := make(chan string, 1)

	// Register the channel for this username lookup
	pendingUserMutex.Lock()
	pendingUserLookups[username] = resultChan
	pendingUserMutex.Unlock()

	// Make sure to clean up when we're done
	defer func() {
		pendingUserMutex.Lock()
		delete(pendingUserLookups, username)
		pendingUserMutex.Unlock()
	}()

	// Create find request
	findReq := Message{
		Type:    CmdFindUserInfo,
		Sender:  m.node.address,
		Content: username,
	}
	reqData, _ := json.Marshal(findReq)

	// Track queried nodes
	queriedNodes := make(map[string]bool)

	// Count how many queries we actually sent
	queriesSent := 0

	// Query each node
	for _, node := range closestNodes {
		if node.ID.Equal(m.node.dht.LocalID) || queriedNodes[node.Address] {
			continue
		}

		queriedNodes[node.Address] = true
		queriesSent++

		go func(nodeAddr string) {
			// Try to get existing connection
			if conn, ok := m.node.getPeerByAddress(nodeAddr); ok {
				conn.Write(reqData)
			} else {
				// Try to establish connection
				if conn, err := net.Dial("tcp", nodeAddr); err == nil {
					conn.Write(reqData)
					m.node.peersMutex.Lock()
					m.node.peers[nodeAddr] = conn
					m.node.peersMutex.Unlock()
					go m.node.handlePeer(conn)
				}
			}
		}(node.Address)
	}

	// If we couldn't query any nodes, fail immediately
	if queriesSent == 0 {
		return "", false
	}

	// Wait for result or timeout
	select {
	case result := <-resultChan:
		// Store locally for future queries
		m.node.dht.StoreValue(userKey, result)
		return result, true
	case <-time.After(5 * time.Second): // Increased timeout
		return "", false
	}
}

// StorePhoto stores a photo chunk and distributes it across the network
func (m *DHTStorageManager) StorePhoto(photo *storage.EncryptedContent) error {
	// Store locally first
	if err := m.storage.StoreContent(photo); err != nil {
		return err
	}

	// Determine which nodes should store this photo
	photoKey := fmt.Sprintf("photo:%s:%s:%d", photo.RecipientID, photo.ID, photo.ChunkIndex)
	photoHash := HashString(photoKey)
	targetID := dht.NodeID(photoHash)

	// Find closest nodes
	closestNodes := m.node.dht.FindClosestNodes(targetID, ReplicationFactor)

	// Serialize the photo
	photoData, err := json.Marshal(photo)
	if err != nil {
		return fmt.Errorf("failed to marshal photo: %w", err)
	}

	// Create store message
	storeMsg := Message{
		Type:    CmdStoreContent,
		Sender:  m.node.address,
		Content: string(photoData),
	}
	msgData, _ := json.Marshal(storeMsg)

	// Distribute to target nodes
	for _, node := range closestNodes {
		if node.ID.Equal(m.node.dht.LocalID) {
			continue // Skip ourselves
		}

		// Send to node
		if conn, ok := m.node.getPeerByAddress(node.Address); ok {
			conn.Write(msgData)
		} else {
			// Try to establish connection
			if conn, err := net.Dial("tcp", node.Address); err == nil {
				conn.Write(msgData)
				m.node.peersMutex.Lock()
				m.node.peers[node.Address] = conn
				m.node.peersMutex.Unlock()
				go m.node.handlePeer(conn)
			}
		}
	}

	return nil
}

// FetchPhoto tries to find a photo across the network
func (m *DHTStorageManager) FetchPhoto(userID, photoID string, chunkIndex int) (*storage.EncryptedContent, error) {
	// Try local storage first
	photos, err := m.storage.GetContentByType(userID, storage.TypePhoto)
	if err == nil {
		for _, photo := range photos {
			if photo.ID == photoID && photo.ChunkIndex == chunkIndex {
				return photo, nil
			}
		}
	}

	// Not found locally, search the network
	photoKey := fmt.Sprintf("photo:%s:%s:%d", userID, photoID, chunkIndex)
	targetID := dht.NodeID(HashString(photoKey))

	// Find nodes that might have this photo
	closestNodes := m.node.dht.FindClosestNodes(targetID, ReplicationFactor*2)

	// Create find photo request
	findReq := Message{
		Type:    CmdFindPhoto,
		Sender:  m.node.address,
		Content: fmt.Sprintf("%s:%s:%d", userID, photoID, chunkIndex),
	}
	reqData, _ := json.Marshal(findReq)

	// Track queried nodes
	queriedNodes := make(map[string]bool)

	// Set up channel for results
	resultChan := make(chan *storage.EncryptedContent, 1)

	// Query each node
	for _, node := range closestNodes {
		if node.ID.Equal(m.node.dht.LocalID) || queriedNodes[node.Address] {
			continue
		}

		queriedNodes[node.Address] = true

		go func(nodeAddr string) {
			if conn, ok := m.node.getPeerByAddress(nodeAddr); ok {
				conn.Write(reqData)
			} else {
				if conn, err := net.Dial("tcp", nodeAddr); err == nil {
					conn.Write(reqData)
					m.node.peersMutex.Lock()
					m.node.peers[nodeAddr] = conn
					m.node.peersMutex.Unlock()
					go m.node.handlePeer(conn)
				}
			}
		}(node.Address)
	}

	// Wait for result or timeout
	select {
	case result := <-resultChan:
		// Store locally for future queries
		m.storage.StoreContent(result)
		return result, nil
	case <-time.After(2 * time.Second):
		return nil, fmt.Errorf("photo not found in network")
	}
}
