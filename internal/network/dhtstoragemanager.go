package network

import (
	"encoding/json"
	"fmt"
	"net"
	"subworld-network/internal/dht"
	"subworld-network/internal/storage"
	"time"
)

const (
	// ReplicationFactor defines how many nodes should store each piece of content
	ReplicationFactor = 5
)

// DHTStorageManager handles content distribution across the network
type DHTStorageManager struct {
	node    *Node
	storage *storage.NodeStorage
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
