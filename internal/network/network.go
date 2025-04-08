package network

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"subworld-network/internal/dht"
	"subworld-network/internal/storage"
)

var (
	pendingUserLookups = make(map[string]chan string)
	pendingUserMutex   sync.RWMutex
)

// Constants for node types and commands
const (
	BootstrapNode         = "BOOTSTRAP"
	RegularNode           = "REGULAR"
	CmdFindAllUserContent = "FIND_ALL_USER_CONTENT"
	CmdJoinNetwork        = "JOIN_NETWORK"
	CmdRequestPeers       = "REQUEST_PEERS"
	CmdPeersList          = "PEERS_LIST"
	CmdHandshake          = "HANDSHAKE"
	CmdMessage            = "MESSAGE"
	CmdStoreContent       = "STORE_CONTENT"
	CmdFindContent        = "FIND_CONTENT"
	CmdContentFound       = "CONTENT_FOUND"
	CmdFindUserInfo       = "FIND_USER_INFO"
	CmdUserInfoResult     = "USER_INFO_RESULT"
	CmdFindPhoto          = "FIND_PHOTO"
	CmdPhotoResult        = "PHOTO_RESULT"

	CmdFindFile   = "FIND_FILE"
	CmdFileResult = "FILE_RESULT"

	CmdFindVoiceStream   = "FIND_VOICE_STREAM"
	CmdVoiceStreamResult = "VOICE_STREAM_RESULT"
)

// NodeConfig contains the configuration for a node
type NodeConfig struct {
	BootstrapNodes []string // List of bootstrap node IPs
	ForceBootstrap bool     // Force this node to be a bootstrap node
	ListenPort     int      // Port to listen on (default: 8080)
	MaxPeers       int      // Maximum number of peers (default: 100)
}

type NodeInfoMessage struct {
	ID        string `json:"id"`
	Address   string `json:"address"`
	PublicKey string `json:"public_key,omitempty"`
}

// Message represents a structured message between nodes
type Message struct {
	Type    string `json:"type"`
	Sender  string `json:"sender"`
	Content string `json:"content"`
}

// Node represents a network node
type Node struct {
	nodeType       string
	address        string // IP:port of this node
	bootstrapNodes []string
	peers          map[string]net.Conn
	peersMutex     sync.RWMutex
	listener       net.Listener
	isRunning      bool
	dht            *dht.DHT             // DHT integration
	storage        *storage.NodeStorage // Local storage
	maxPeers       int                  // Maximum number of peers
}

func (n *Node) getPeerByAddress(address string) (net.Conn, bool) {
	n.peersMutex.RLock()
	defer n.peersMutex.RUnlock()

	conn, exists := n.peers[address]
	return conn, exists
}

// HashString creates a SHA1 hash from a string
func HashString(s string) [20]byte {
	return sha1.Sum([]byte(s))
}

// NewNode creates a new node
func NewNode(config NodeConfig) (*Node, error) {
	// Set default values if not provided
	if config.ListenPort == 0 {
		config.ListenPort = 8080
	}
	if config.MaxPeers == 0 {
		config.MaxPeers = 100
	}

	// Get public IP
	publicIP, err := getPublicIP()
	if err != nil {
		fmt.Printf("Warning: Failed to determine public IP: %v\n", err)
		// Fallback to local IP if public IP detection fails
		localIP, localErr := getLocalIP()
		if localErr != nil {
			return nil, fmt.Errorf("failed to get any valid IP: %w", localErr)
		}
		publicIP = localIP
	}

	fmt.Printf("Using IP: %s\n", publicIP)
	address := fmt.Sprintf("%s:%d", publicIP, config.ListenPort)

	// Determine node type based on IP or forced mode
	nodeType := RegularNode

	if config.ForceBootstrap {
		nodeType = BootstrapNode
		fmt.Println("IMPORTANT: Running in FORCED BOOTSTRAP MODE")
	} else {
		// Check if our IP is in the bootstrap list
		for _, bootstrapAddr := range config.BootstrapNodes {
			bootstrapIP := strings.Split(bootstrapAddr, ":")[0]
			if publicIP == bootstrapIP {
				nodeType = BootstrapNode
				fmt.Printf("IP %s matches bootstrap node list, running as BOOTSTRAP node\n", publicIP)
				break
			}
		}
	}

	node := &Node{
		nodeType:       nodeType,
		address:        address,
		bootstrapNodes: config.BootstrapNodes,
		peers:          make(map[string]net.Conn),
		maxPeers:       config.MaxPeers,
	}

	fmt.Printf("Node created: Type=%s, Address=%s\n", node.nodeType, node.address)
	return node, nil
}

// SetStorage sets the storage backend
func (n *Node) SetStorage(storage *storage.NodeStorage) {
	n.storage = storage
}

// GetNodeType returns the node type
func (n *Node) GetNodeType() string {
	return n.nodeType
}

// GetAddress returns the node address
func (n *Node) GetAddress() string {
	return n.address
}

// GetDHT returns the DHT instance
func (n *Node) GetDHT() *dht.DHT {
	return n.dht
}

// Run starts the node operation
func (n *Node) Run() error {
	if n.isRunning {
		return fmt.Errorf("node is already running")
	}

	// Initialize DHT
	n.initializeDHT()

	// Start TCP listener for all nodes
	port := strings.Split(n.address, ":")[1]
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		return fmt.Errorf("failed to start listener: %w", err)
	}
	n.listener = listener
	n.isRunning = true

	fmt.Printf("Node started as %s on %s\n", n.nodeType, n.address)

	// Accept connections in a goroutine
	go n.acceptConnections()

	// If this is a regular node, connect to bootstrap nodes
	if n.nodeType == RegularNode {
		go n.connectToBootstrapNodes()
	} else {
		fmt.Println("Running as bootstrap node - waiting for connections")
	}

	// Start heartbeat to maintain peer list
	go n.startHeartbeat()

	return nil
}

// Stop gracefully shuts down the node
func (n *Node) Stop() {
	if !n.isRunning {
		return
	}

	// Close listener
	if n.listener != nil {
		n.listener.Close()
	}

	// Close all peer connections
	n.peersMutex.Lock()
	for addr, conn := range n.peers {
		conn.Close()
		delete(n.peers, addr)
	}
	n.peersMutex.Unlock()

	n.isRunning = false
	fmt.Println("Node stopped")
}

// GetPublicIP attempts to get the public IP address by checking various services
func getPublicIP() (string, error) {
	// Try multiple services to find public IP
	ipServices := []string{
		"https://api.ipify.org",
		"https://ifconfig.me/ip",
		"https://icanhazip.com",
		"https://ident.me",
		"https://ipecho.net/plain",
	}

	for _, service := range ipServices {
		fmt.Printf("Trying to get public IP from: %s\n", service)
		resp, err := http.Get(service)
		if err != nil {
			fmt.Printf("Service %s failed: %v\n", service, err)
			continue
		}
		defer resp.Body.Close()

		ip, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Printf("Reading from %s failed: %v\n", service, err)
			continue
		}

		// Clean the IP string
		ipStr := strings.TrimSpace(string(ip))

		// Validate that it's a valid IP
		parsedIP := net.ParseIP(ipStr)
		if parsedIP != nil {
			fmt.Printf("Public IP found: %s\n", ipStr)
			return ipStr, nil
		}
	}

	return "", fmt.Errorf("could not determine public IP from any service")
}

// getLocalIP returns the non-loopback local IP of the host
func getLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, address := range addrs {
		// Check the address type and if it's not a loopback
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", fmt.Errorf("no non-loopback IPv4 address found")
}

// Initialize DHT for a node
func (n *Node) initializeDHT() {
	fmt.Println("Initializing DHT...")
	n.dht = dht.NewDHT(n.address)

	// Start periodic DHT maintenance
	go n.dhtMaintenance()
}

// dhtMaintenance performs periodic DHT-related tasks
func (n *Node) dhtMaintenance() {
	ticker := time.NewTicker(15 * time.Minute)
	defer ticker.Stop()

	for n.isRunning {
		select {
		case <-ticker.C:
			// Refresh buckets that haven't been refreshed recently
			n.dht.RefreshAllBuckets()

			// Republish our own info
			n.publishNodeToDHT()

			// Check for messages addressed to us
			n.checkDHTMessages()
		}
	}
}

// publishNodeToDHT publishes this node's information to the DHT
func (n *Node) publishNodeToDHT() {
	// Store our node info under a well-known key format
	key := fmt.Sprintf("node:%s", n.dht.LocalID.String())

	// Create node info JSON
	nodeInfo := NodeInfoMessage{
		ID:        n.dht.LocalID.String(),
		Address:   n.address,
		PublicKey: "", // Add your public key here if using encryption
	}

	nodeInfoJSON, err := json.Marshal(nodeInfo)
	if err != nil {
		fmt.Printf("Failed to marshal node info: %v\n", err)
		return
	}

	// Store locally
	n.dht.StoreValue(key, string(nodeInfoJSON))

	// Broadcast to closest nodes
	closestNodes := n.dht.FindClosestNodes(n.dht.LocalID, dht.K)

	storeMsg := Message{
		Type:    CmdStoreContent,
		Sender:  n.address,
		Content: string(nodeInfoJSON),
	}
	storeData, _ := json.Marshal(storeMsg)

	for _, node := range closestNodes {
		if peer, ok := n.getPeerByAddress(node.Address); ok {
			peer.Write(storeData)
		}
	}
}

// checkDHTMessages checks for messages addressed to this node in the DHT
func (n *Node) checkDHTMessages() {
	// Check for messages addressed to us
	messageKey := fmt.Sprintf("msgs:%s", n.dht.LocalID.String())

	// In a real implementation, you'd do a DHT lookup for this key
	// For now, we'll just check our local store
	if messages, found := n.dht.GetValue(messageKey); found {
		fmt.Printf("Found messages for us: %s\n", messages)
		// Process the messages...

		// Clear the messages
		n.dht.StoreValue(messageKey, "")
	}
}

// connectToBootstrapNodes attempts to connect to bootstrap nodes
func (n *Node) connectToBootstrapNodes() {
	for _, bootstrapAddr := range n.bootstrapNodes {
		// Skip if this is our own address
		if strings.HasPrefix(bootstrapAddr, strings.Split(n.address, ":")[0]) {
			continue
		}

		// Ensure the address has a port
		if !strings.Contains(bootstrapAddr, ":") {
			bootstrapAddr = bootstrapAddr + ":8080"
		}

		// Try to connect
		fmt.Printf("Attempting to connect to bootstrap node: %s\n", bootstrapAddr)
		conn, err := net.Dial("tcp", bootstrapAddr)
		if err != nil {
			fmt.Printf("Failed to connect to bootstrap node %s: %v\n", bootstrapAddr, err)
			continue
		}

		// Send JOIN_NETWORK message
		joinMsg := Message{
			Type:    CmdJoinNetwork,
			Sender:  n.address,
			Content: "",
		}
		msgData, _ := json.Marshal(joinMsg)
		_, err = conn.Write(msgData)
		if err != nil {
			fmt.Printf("Failed to send JOIN_NETWORK to %s: %v\n", bootstrapAddr, err)
			conn.Close()
			continue
		}

		// Add to peers
		n.peersMutex.Lock()
		n.peers[bootstrapAddr] = conn
		n.peersMutex.Unlock()

		fmt.Printf("Connected to bootstrap node: %s\n", bootstrapAddr)

		// Handle messages from this peer
		go n.handlePeer(conn)

		// We only need to connect to one bootstrap node initially
		// The DHT will help us find more peers
		break
	}
}

// acceptConnections handles incoming connections
func (n *Node) acceptConnections() {
	for n.isRunning {
		conn, err := n.listener.Accept()
		if err != nil {
			if !n.isRunning {
				return
			}
			fmt.Printf("Failed to accept connection: %v\n", err)
			continue
		}

		// Check if we've reached max peers
		n.peersMutex.RLock()
		peerCount := len(n.peers)
		n.peersMutex.RUnlock()

		if peerCount >= n.maxPeers && n.nodeType != BootstrapNode {
			fmt.Printf("Rejected connection: max peers (%d) reached\n", n.maxPeers)
			conn.Close()
			continue
		}

		// Handle the connection in a new goroutine
		go n.handlePeer(conn)
	}
}

// handlePeer handles communication with a peer
func (n *Node) handlePeer(conn net.Conn) {
	defer func() {
		conn.Close()
		n.peersMutex.Lock()
		delete(n.peers, conn.RemoteAddr().String())
		n.peersMutex.Unlock()
	}()

	buffer := make([]byte, 4096)
	for n.isRunning {
		// Read message
		nRead, err := conn.Read(buffer)
		if err != nil {
			fmt.Printf("Error reading from %s: %v\n", conn.RemoteAddr().String(), err)
			return
		}

		// Parse message
		var msg Message
		if err := json.Unmarshal(buffer[:nRead], &msg); err != nil {
			fmt.Printf("Invalid message from %s: %v\n", conn.RemoteAddr().String(), err)
			continue
		}

		// Handle message based on type
		switch msg.Type {
		case CmdFindAllUserContent:
			n.handleFindAllUserContent(conn, msg)
		case CmdJoinNetwork:
			n.handleJoinNetwork(conn, msg)
		case CmdRequestPeers:
			n.handleRequestPeers(conn, msg)
		case CmdPeersList:
			n.handlePeersList(msg)
		case CmdHandshake:
			n.handleHandshake(conn, msg)
		case CmdMessage:
			// Regular message, broadcast to other peers
			fmt.Printf("Message from %s: %s\n", msg.Sender, msg.Content)
			n.broadcastMessage(msg)
		case CmdStoreContent:
			n.handleStoreContent(conn, msg)
		case CmdFindContent:
			n.handleFindContent(conn, msg)
		case CmdContentFound:
			n.handleContentFound(msg)
		case CmdFindUserInfo:
			n.handleFindUserInfo(conn, msg)
		case CmdUserInfoResult:
			n.handleUserInfoResult(conn, msg)
		case CmdFindPhoto:
			n.handleFindPhoto(conn, msg)
		case CmdPhotoResult:
			n.handlePhotoResult(conn, msg)
		case CmdFindFile:
			n.handleFindFile(conn, msg)
		case CmdFileResult:
			n.handleFileResult(conn, msg)
		case CmdFindVoiceStream:
			n.handleFindVoiceStream(conn, msg)
		case CmdVoiceStreamResult:
			n.handleVoiceStreamResult(conn, msg)
		default:
			fmt.Printf("Unknown message type: %s\n", msg.Type)
		}
	}
}

func (n *Node) handleFindAllUserContent(conn net.Conn, msg Message) {
	userID := msg.Content

	// Get all content for this user
	var allContent []*storage.EncryptedContent

	// Get messages
	messages, err := n.storage.GetMessagesByUser(userID, false)
	if err == nil {
		allContent = append(allContent, messages...)
	}

	// Get photos
	photos, err := n.storage.GetContentByType(userID, storage.TypePhoto)
	if err == nil {
		allContent = append(allContent, photos...)
	}

	// Get voice messages
	voiceMessages, err := n.storage.GetContentByType(userID, storage.TypeVoiceMessage)
	if err == nil {
		allContent = append(allContent, voiceMessages...)
	}

	// Don't include call signals - they're ephemeral

	// If we have content, send it back
	if len(allContent) > 0 {
		// Send each content item separately to avoid large messages
		for _, content := range allContent {
			contentData, err := json.Marshal(content)
			if err != nil {
				continue
			}

			response := Message{
				Type:    CmdStoreContent, // Receiver will store this content
				Sender:  n.address,
				Content: string(contentData),
			}

			responseData, _ := json.Marshal(response)
			conn.Write(responseData)

			// Small delay to avoid overwhelming the connection
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// handleFindFile handles requests to find a file
func (n *Node) handleFindFile(conn net.Conn, msg Message) {
	// Parse parameters
	parts := strings.Split(msg.Content, ":")
	if len(parts) != 3 {
		return
	}

	userID := parts[0]
	fileID := parts[1]
	chunkIndex, err := strconv.Atoi(parts[2])
	if err != nil {
		return
	}

	// Look up in local storage
	if n.storage == nil {
		return
	}

	files, err := n.storage.GetContentByType(userID, storage.TypeFile)
	if err != nil {
		return
	}

	// Find matching file
	for _, file := range files {
		if file.ID == fileID && file.ChunkIndex == chunkIndex {
			// Found it, send back
			fileData, err := json.Marshal(file)
			if err != nil {
				return
			}

			response := Message{
				Type:    CmdFileResult,
				Sender:  n.address,
				Content: string(fileData),
			}

			responseData, _ := json.Marshal(response)
			conn.Write(responseData)
			return
		}
	}
}

// handleFileResult handles file lookup results
func (n *Node) handleFileResult(conn net.Conn, msg Message) {
	// Parse result
	var file storage.EncryptedContent
	if err := json.Unmarshal([]byte(msg.Content), &file); err != nil {
		fmt.Printf("Invalid file result: %v\n", err)
		return
	}

	// Store locally
	if n.storage != nil {
		n.storage.StoreContent(&file)
		fmt.Printf("Received file %s chunk %d for user %s from %s\n",
			file.ID, file.ChunkIndex, file.RecipientID, msg.Sender)
	}
}

// handleJoinNetwork handles a request to join the network
func (n *Node) handleJoinNetwork(conn net.Conn, msg Message) {
	fmt.Printf("Join request from %s\n", msg.Sender)

	// Add the new peer
	n.peersMutex.Lock()
	n.peers[msg.Sender] = conn
	n.peersMutex.Unlock()

	// Send handshake back
	handshakeMsg := Message{
		Type:    CmdHandshake,
		Sender:  n.address,
		Content: "",
	}
	msgData, _ := json.Marshal(handshakeMsg)
	conn.Write(msgData)

	// Send the peer list to the new node
	n.sendPeerList(conn)

	// Add the new node to our DHT routing table
	// Extract the node ID from the sender address
	nodeID := sha1.Sum([]byte(msg.Sender))
	n.dht.AddNode(nodeID, msg.Sender, "")

	// Broadcast to other peers that a new node joined
	n.broadcastPeerList()
}

// handleRequestPeers sends the peer list to the requesting peer
func (n *Node) handleRequestPeers(conn net.Conn, msg Message) {
	n.sendPeerList(conn)
}

// handlePeersList processes a received peer list
func (n *Node) handlePeersList(msg Message) {
	var peerList []string
	err := json.Unmarshal([]byte(msg.Content), &peerList)
	if err != nil {
		fmt.Printf("Failed to parse peer list: %v\n", err)
		return
	}

	// Connect to new peers
	for _, peerAddr := range peerList {
		// Skip if we already know this peer or if it's ourselves
		if peerAddr == n.address || n.isPeerConnected(peerAddr) {
			continue
		}

		// Add to DHT routing table
		nodeID := sha1.Sum([]byte(peerAddr))
		n.dht.AddNode(nodeID, peerAddr, "")

		// Check if we've reached max peers before connecting to more
		n.peersMutex.RLock()
		peerCount := len(n.peers)
		n.peersMutex.RUnlock()

		if peerCount >= n.maxPeers && n.nodeType != BootstrapNode {
			fmt.Printf("Not connecting to new peer %s: max peers (%d) reached\n",
				peerAddr, n.maxPeers)
			continue
		}

		// Try to connect to the new peer
		go n.connectToPeer(peerAddr)
	}
}

// handleHandshake processes a handshake message
func (n *Node) handleHandshake(conn net.Conn, msg Message) {
	// Add the peer to our list if not already there
	n.peersMutex.Lock()
	n.peers[msg.Sender] = conn
	n.peersMutex.Unlock()

	// Also add to DHT routing table
	nodeID := sha1.Sum([]byte(msg.Sender))
	n.dht.AddNode(nodeID, msg.Sender, "")

	fmt.Printf("Handshake completed with %s\n", msg.Sender)
}

// handleStoreContent handles a request to store content
func (n *Node) handleStoreContent(conn net.Conn, msg Message) {
	// First, check if this is a user info message
	var contentMap map[string]interface{}
	if err := json.Unmarshal([]byte(msg.Content), &contentMap); err == nil {
		// This might be a user info message
		if contentType, ok := contentMap["type"].(string); ok && contentType == "user_info" {
			if key, ok := contentMap["key"].(string); ok {
				if valueRaw, ok := contentMap["value"]; ok {
					// Convert value to string if needed
					var valueStr string
					switch v := valueRaw.(type) {
					case string:
						valueStr = v
					default:
						// Try to convert to JSON string
						if valueBytes, err := json.Marshal(v); err == nil {
							valueStr = string(valueBytes)
						}
					}

					if valueStr != "" {
						// Store in DHT
						n.dht.StoreValue(key, valueStr)
						fmt.Printf("Stored user info for key: %s\n", key)
						return
					}
				}
			}
		}
	}

	// If we have storage configured, store the content
	if n.storage != nil {
		var content storage.EncryptedContent
		if err := json.Unmarshal([]byte(msg.Content), &content); err != nil {
			fmt.Printf("Invalid content in STORE_CONTENT message: %v\n", err)
			return
		}

		// Check if we already have this content
		existingMessages, _ := n.storage.GetMessagesByUser(content.RecipientID, true)
		for _, existing := range existingMessages {
			if existing.ID == content.ID && existing.Type == content.Type {
				// Already have this content, no need to store again
				fmt.Printf("Content ID %s already exists locally, not storing duplicate\n", content.ID)
				return
			}
		}

		// Store the content
		err := n.storage.StoreContent(&content)
		if err != nil {
			fmt.Printf("Failed to store content: %v\n", err)
			return
		}

		fmt.Printf("Stored content ID %s for user %s\n", content.ID, content.RecipientID)

		// Forward to other nodes that should have this content (except the sender)
		// This helps with network propagation
		if shouldForward := rand.Intn(10) < 3; shouldForward { // 30% chance to forward
			contentKey := fmt.Sprintf("content:%s:%s", content.RecipientID, content.ID)
			targetID := dht.NodeID(HashString(contentKey))
			closestNodes := n.dht.FindClosestNodes(targetID, 3) // Forward to 3 nodes

			for _, node := range closestNodes {
				if node.ID.Equal(n.dht.LocalID) || node.Address == msg.Sender {
					continue
				}

				if conn, ok := n.getPeerByAddress(node.Address); ok {
					forwardMsg := Message{
						Type:    CmdStoreContent,
						Sender:  n.address,
						Content: msg.Content,
					}
					forwardData, _ := json.Marshal(forwardMsg)
					conn.Write(forwardData)
				}
			}
		}
	} else {
		// If no storage configured, just keep in DHT
		contentID := fmt.Sprintf("content:%s", time.Now().Format(time.RFC3339Nano))
		n.dht.StoreValue(contentID, msg.Content)
	}
}

// handleFindContent handles a request to find content
func (n *Node) handleFindContent(conn net.Conn, msg Message) {
	// Parse the content ID from the message
	parts := strings.Split(msg.Content, ":")
	if len(parts) != 2 {
		fmt.Printf("Invalid FIND_CONTENT message format: %s\n", msg.Content)
		return
	}

	userID := parts[0]
	contentID := parts[1]

	// Check if we have the content
	var foundContent *storage.EncryptedContent
	var found bool

	if n.storage != nil {
		// Look in our local storage
		messages, err := n.storage.GetMessagesByUser(userID, false)
		if err == nil {
			for _, content := range messages {
				if content.ID == contentID {
					foundContent = content
					found = true
					break
				}
			}
		}
	}

	// If found, send it back
	if found && foundContent != nil {
		contentData, err := json.Marshal(foundContent)
		if err != nil {
			fmt.Printf("Failed to marshal content: %v\n", err)
			return
		}

		response := Message{
			Type:    CmdContentFound,
			Sender:  n.address,
			Content: string(contentData),
		}

		responseData, _ := json.Marshal(response)
		conn.Write(responseData)
	} else {
		// Not found, could forward the request to other nodes
		fmt.Printf("Content %s for user %s not found locally\n", contentID, userID)
	}
}

// handleContentFound processes a response with found content
func (n *Node) handleContentFound(msg Message) {
	// Parse the content
	var content storage.EncryptedContent
	if err := json.Unmarshal([]byte(msg.Content), &content); err != nil {
		fmt.Printf("Invalid content in CONTENT_FOUND message: %v\n", err)
		return
	}

	fmt.Printf("Received content %s for user %s from %s\n",
		content.ID, content.RecipientID, msg.Sender)

	// Store locally if we have storage
	if n.storage != nil {
		n.storage.StoreContent(&content)
	}

	// Additional processing could be done here, like notifying waiting requests
}

// sendPeerList sends our list of peers to the specified connection
func (n *Node) sendPeerList(conn net.Conn) {
	// Get the current peer list from both direct connections and DHT
	var peerList []string

	// Add directly connected peers
	n.peersMutex.RLock()
	for peer := range n.peers {
		peerList = append(peerList, peer)
	}
	n.peersMutex.RUnlock()

	// Add ourselves
	peerList = append(peerList, n.address)

	// Add peers from DHT routing table
	dhtPeers := n.dht.GetAllPeers()
	for _, peer := range dhtPeers {
		// Check if already in the list
		exists := false
		for _, existingPeer := range peerList {
			if existingPeer == peer.Address {
				exists = true
				break
			}
		}

		if !exists {
			peerList = append(peerList, peer.Address)
		}
	}

	// Serialize the peer list
	peerListData, err := json.Marshal(peerList)
	if err != nil {
		fmt.Printf("Failed to marshal peer list: %v\n", err)
		return
	}

	// Send the peer list
	msg := Message{
		Type:    CmdPeersList,
		Sender:  n.address,
		Content: string(peerListData),
	}
	msgData, _ := json.Marshal(msg)
	conn.Write(msgData)
}

// broadcastPeerList sends the peer list to all connected peers
func (n *Node) broadcastPeerList() {
	// Get the current peer list
	var peerList []string

	// Add directly connected peers
	n.peersMutex.RLock()
	for peer := range n.peers {
		peerList = append(peerList, peer)
	}
	peerList = append(peerList, n.address)
	n.peersMutex.RUnlock()

	// Add peers from DHT routing table
	dhtPeers := n.dht.GetAllPeers()
	for _, peer := range dhtPeers {
		// Check if already in the list
		exists := false
		for _, existingPeer := range peerList {
			if existingPeer == peer.Address {
				exists = true
				break
			}
		}

		if !exists {
			peerList = append(peerList, peer.Address)
		}
	}

	// Limit peer list size to avoid massive messages
	if len(peerList) > 100 {
		peerList = peerList[:100]
	}

	// Serialize the peer list
	peerListData, err := json.Marshal(peerList)
	if err != nil {
		fmt.Printf("Failed to marshal peer list: %v\n", err)
		return
	}

	// Create the message
	msg := Message{
		Type:    CmdPeersList,
		Sender:  n.address,
		Content: string(peerListData),
	}

	// Broadcast it
	n.broadcastMessage(msg)
}

// isPeerConnected checks if we're already connected to a peer
func (n *Node) isPeerConnected(peerAddr string) bool {
	n.peersMutex.RLock()
	defer n.peersMutex.RUnlock()
	_, exists := n.peers[peerAddr]
	return exists
}

// connectToPeer attempts to connect to a peer
func (n *Node) connectToPeer(peerAddr string) {
	// Ensure the address has a port
	if !strings.Contains(peerAddr, ":") {
		peerAddr = peerAddr + ":8080"
	}

	// Try to connect
	conn, err := net.Dial("tcp", peerAddr)
	if err != nil {
		fmt.Printf("Failed to connect to peer %s: %v\n", peerAddr, err)
		return
	}

	// Send handshake
	handshakeMsg := Message{
		Type:    CmdHandshake,
		Sender:  n.address,
		Content: "",
	}
	msgData, _ := json.Marshal(handshakeMsg)
	_, err = conn.Write(msgData)
	if err != nil {
		fmt.Printf("Failed to send handshake to %s: %v\n", peerAddr, err)
		conn.Close()
		return
	}

	// Add to peers
	n.peersMutex.Lock()
	n.peers[peerAddr] = conn
	n.peersMutex.Unlock()

	// Add to DHT routing table
	nodeID := sha1.Sum([]byte(peerAddr))
	n.dht.AddNode(nodeID, peerAddr, "")

	fmt.Printf("Connected to peer: %s\n", peerAddr)

	// Handle messages from this peer
	go n.handlePeer(conn)
}

// BroadcastMessage broadcasts a text message to all peers
func (n *Node) BroadcastMessage(text string) {
	msg := Message{
		Type:    CmdMessage,
		Sender:  n.address,
		Content: text,
	}
	n.broadcastMessage(msg)

}

// broadcastMessage sends a message to all connected peers except the sender
func (n *Node) broadcastMessage(msg Message) {
	msgData, err := json.Marshal(msg)
	if err != nil {
		fmt.Printf("Failed to marshal message: %v\n", err)
		return
	}

	n.peersMutex.RLock()
	defer n.peersMutex.RUnlock()

	for addr, conn := range n.peers {
		if addr != msg.Sender {
			_, err := conn.Write(msgData)
			if err != nil {
				fmt.Printf("Failed to send message to %s: %v\n", addr, err)
			}
		}
	}
}

// sendMessageToPeer sends a message to a specific peer
func (n *Node) sendMessageToPeer(peerAddr string, msg Message) error {
	// Check if we're already connected
	n.peersMutex.RLock()
	conn, connected := n.peers[peerAddr]
	n.peersMutex.RUnlock()

	if !connected {
		// Try to connect
		newConn, err := net.Dial("tcp", peerAddr)
		if err != nil {
			return fmt.Errorf("failed to connect to peer %s: %w", peerAddr, err)
		}

		// Add to peers
		n.peersMutex.Lock()
		n.peers[peerAddr] = newConn
		n.peersMutex.Unlock()

		// Start handling messages
		go n.handlePeer(newConn)

		conn = newConn
	}

	// Serialize and send message
	msgData, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	_, err = conn.Write(msgData)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

// startHeartbeat periodically sends peer list updates and removes dead connections
func (n *Node) startHeartbeat() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for n.isRunning {
		select {
		case <-ticker.C:
			// Print connected peers
			n.printConnectedPeers()

			// Request peers list from bootstrap nodes if we're a regular node with no peers
			if n.nodeType == RegularNode && len(n.peers) == 0 {
				n.connectToBootstrapNodes()
			}

			// If we're a bootstrap node, broadcast the peer list
			if n.nodeType == BootstrapNode {
				n.broadcastPeerList()
			}

			// Refresh DHT buckets
			n.RefreshDHT()
		}
	}
}

// RefreshDHT refreshes the DHT buckets
func (n *Node) RefreshDHT() {
	n.dht.RefreshAllBuckets()
}

// printConnectedPeers displays all connected peers
func (n *Node) printConnectedPeers() {
	// Get directly connected peers
	n.peersMutex.RLock()
	directPeers := make(map[string]bool)
	for addr := range n.peers {
		directPeers[addr] = true
	}
	directPeerCount := len(directPeers)
	n.peersMutex.RUnlock()

	// Get DHT peers
	dhtPeers := n.dht.GetAllPeers()

	fmt.Printf("Connected peers (%d direct, %d in DHT):\n", directPeerCount, len(dhtPeers))

	// Print direct peers
	fmt.Println("Direct connections:")
	for addr := range directPeers {
		fmt.Printf("  - %s\n", addr)
	}

	// Print DHT peers (limit to 10 for cleaner output)
	fmt.Println("DHT routing table (sample):")
	peerCount := 0
	for _, peer := range dhtPeers {
		if peerCount >= 10 {
			fmt.Printf("  - ... and %d more\n", len(dhtPeers)-10)
			break
		}
		fmt.Printf("  - %s (ID: %s)\n", peer.Address, peer.ID.String()[:8])
		peerCount++
	}
}

func (n *Node) FindUser(username string) (string, bool) {
	// Create a storage manager
	storageManager := NewDHTStorageManager(n, n.storage)

	// Use the enhanced distributed lookup
	return storageManager.FindUser(username)
}

// RegisterUser registers a username in the DHT
func (n *Node) RegisterUser(username string, publicKey string) error {
	// Create user info with our address and public key
	userInfo := fmt.Sprintf("{\"username\":\"%s\",\"address\":\"%s\",\"public_key\":\"%s\"}",
		username, n.address, publicKey)

	// Create a storage manager
	storageManager := NewDHTStorageManager(n, n.storage)

	// Register using the distributed approach
	return storageManager.RegisterUser(username, userInfo)
}

// GetUserMessages retrieves messages for a user
func (n *Node) GetUserMessages(userID string) ([]*storage.EncryptedContent, error) {
	if n.storage == nil {
		return nil, fmt.Errorf("no storage configured")
	}

	return n.storage.GetMessagesByUser(userID, false)
}

// GetContentByType retrieves content of a specific type for a user
func (n *Node) GetContentByType(userID string, contentType storage.ContentType) ([]*storage.EncryptedContent, error) {
	if n.storage == nil {
		return nil, fmt.Errorf("no storage configured")
	}

	return n.storage.GetContentByType(userID, contentType)
}

// SendMessageToUser sends a message to a specific user
func (n *Node) SendMessageToUser(username, message string) error {
	// Find user address
	userInfo, found := n.FindUser(username)
	if !found {
		return fmt.Errorf("user %s not found", username)
	}

	// Parse user info
	var userInfoMap map[string]string
	err := json.Unmarshal([]byte(userInfo), &userInfoMap)
	if err != nil {
		return fmt.Errorf("invalid user info: %v", err)
	}

	address, ok := userInfoMap["address"]
	if !ok {
		return fmt.Errorf("user info doesn't contain address")
	}

	// Try to send directly if connected
	n.peersMutex.RLock()
	_, connected := n.peers[address]
	n.peersMutex.RUnlock()

	if connected {
		// Send directly
		msg := Message{
			Type:    CmdMessage,
			Sender:  n.address,
			Content: message,
		}
		msgData, _ := json.Marshal(msg)

		n.peersMutex.RLock()
		conn := n.peers[address]
		n.peersMutex.RUnlock()

		_, err := conn.Write(msgData)
		if err != nil {
			// If direct send fails, store in DHT
			return n.storeMessageInDHT(username, message)
		}
		return nil
	}

	// Not directly connected, store in DHT for later retrieval
	return n.storeMessageInDHT(username, message)
}

// storeMessageInDHT stores a message in the DHT for later retrieval
func (n *Node) storeMessageInDHT(username, message string) error {
	timestamp := time.Now().Unix()
	messageKey := fmt.Sprintf("msgs:%s:%d", username, timestamp)
	messageData := fmt.Sprintf("{\"from\":\"%s\",\"content\":\"%s\",\"time\":%d}",
		n.address, message, timestamp)

	// Store in our local DHT
	n.dht.StoreValue(messageKey, messageData)

	// Also store in the distributed network
	// Find nodes close to this key
	msgID := dht.NodeID(HashString(messageKey))
	closestNodes := n.dht.FindClosestNodes(msgID, 20)

	// Create store content message
	storeMsg := Message{
		Type:   CmdStoreContent,
		Sender: n.address,
		Content: fmt.Sprintf("{\"id\":\"%s\",\"recipient\":\"%s\",\"content\":\"%s\"}",
			messageKey, username, messageData),
	}

	// Send to closest nodes
	for _, node := range closestNodes {
		// Skip if it's ourselves - we already stored it
		if node.ID.Equal(n.dht.LocalID) {
			continue
		}

		// Try to send
		n.sendMessageToPeer(node.Address, storeMsg)
	}

	return nil
}

// UploadPhoto uploads a photo to the DHT
func (n *Node) UploadPhoto(recipientUsername string, photoData []byte, fileName string) (string, error) {
	// Generate message ID
	messageID := fmt.Sprintf("photo-%d", time.Now().UnixNano())

	// Determine MIME type from data
	mimeType := http.DetectContentType(photoData)
	fmt.Printf("Uploading photo with MIME type: %s\n", mimeType)

	// Split photo into chunks if large
	chunkSize := 65536 // 64KB
	chunkCount := (len(photoData) + chunkSize - 1) / chunkSize

	// Find recipient's public key
	userInfo, found := n.FindUser(recipientUsername)
	if !found {
		return "", fmt.Errorf("user %s not found", recipientUsername)
	}

	// Parse user info
	var userInfoMap map[string]string
	err := json.Unmarshal([]byte(userInfo), &userInfoMap)
	if err != nil {
		return "", fmt.Errorf("invalid user info: %v", err)
	}

	recipientID, ok := userInfoMap["public_key"]
	if !ok || recipientID == "" {
		recipientID = userInfoMap["address"] // Fallback to address
	}

	// Create metadata message
	photoMsg := &storage.EncryptedContent{
		ID:            messageID,
		SenderID:      n.address,
		RecipientID:   recipientID,
		Type:          storage.TypePhoto,
		EncryptedData: "", // Metadata only
		Timestamp:     time.Now(),
		TotalChunks:   chunkCount,
		ChunkIndex:    -1, // Metadata indicator
	}

	// Store metadata
	if n.storage != nil {
		if err := n.storage.StoreContent(photoMsg); err != nil {
			return "", fmt.Errorf("failed to store photo metadata: %w", err)
		}
	}

	// Store chunks
	for i := 0; i < chunkCount; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(photoData) {
			end = len(photoData)
		}

		chunk := &storage.EncryptedContent{
			ID:            messageID,
			SenderID:      n.address,
			RecipientID:   recipientID,
			Type:          storage.TypePhoto,
			EncryptedData: string(photoData[start:end]), // In reality, this would be encrypted
			Timestamp:     time.Now(),
			TotalChunks:   chunkCount,
			ChunkIndex:    i,
		}

		// Store locally
		if n.storage != nil {
			if err := n.storage.StoreContent(chunk); err != nil {
				return "", fmt.Errorf("failed to store photo chunk %d: %w", i, err)
			}
		}

		// Also store in DHT (simplified)
		chunkKey := fmt.Sprintf("photo:%s:%d", messageID, i)
		chunkData, _ := json.Marshal(chunk)
		n.dht.StoreValue(chunkKey, string(chunkData))
	}

	return messageID, nil
}

// GetPhoto retrieves a photo from storage
func (n *Node) GetPhoto(userID, photoID string) (*storage.EncryptedContent, []*storage.EncryptedContent, error) {
	if n.storage == nil {
		return nil, nil, fmt.Errorf("no storage configured")
	}

	// Get all photo content for the user
	photos, err := n.storage.GetContentByType(userID, storage.TypePhoto)
	if err != nil {
		return nil, nil, err
	}

	// Find metadata and chunks
	var metadata *storage.EncryptedContent
	var chunks []*storage.EncryptedContent

	for _, content := range photos {
		if content.ID == photoID {
			if content.ChunkIndex == -1 {
				// This is the metadata
				metadata = content
			} else {
				// This is a chunk
				chunks = append(chunks, content)
			}
		}
	}

	// Sort chunks by index
	if chunks != nil {
		sort.Slice(chunks, func(i, j int) bool {
			return chunks[i].ChunkIndex < chunks[j].ChunkIndex
		})
	}

	if metadata == nil {
		return nil, nil, fmt.Errorf("photo %s not found", photoID)
	}

	return metadata, chunks, nil
}

// HandleCallSignal processes a WebRTC call signaling message
func (n *Node) HandleCallSignal(signal *storage.EncryptedContent) error {
	// Store signal with short TTL
	if n.storage != nil {
		signal.TTL = 300 // 5 minute TTL
		if err := n.storage.StoreContent(signal); err != nil {
			return fmt.Errorf("failed to store signal: %w", err)
		}
	}

	// Try to forward directly if recipient is connected
	n.peersMutex.RLock()
	conn, connected := n.peers[signal.RecipientID]
	n.peersMutex.RUnlock()

	if connected {
		// Forward directly
		signalData, _ := json.Marshal(signal)
		msg := Message{
			Type:    "CALL_SIGNAL",
			Sender:  n.address,
			Content: string(signalData),
		}
		msgData, _ := json.Marshal(msg)
		_, err := conn.Write(msgData)
		if err != nil {
			fmt.Printf("Failed to forward call signal: %v\n", err)
		}
	}

	// Also store in the DHT for redundancy
	signalKey := fmt.Sprintf("signal:%s:%s", signal.RecipientID, time.Now().Format(time.RFC3339Nano))
	signalData, _ := json.Marshal(signal)
	n.dht.StoreValue(signalKey, string(signalData))

	return nil
}

// GetPendingCallSignals gets pending call signals for a user
func (n *Node) GetPendingCallSignals(userID string) ([]*storage.EncryptedContent, error) {
	if n.storage == nil {
		return nil, fmt.Errorf("no storage configured")
	}

	return n.storage.GetContentByType(userID, storage.TypeCallSignal)
}

// handleFindVoiceStream handles requests to find voice stream chunks
func (n *Node) handleFindVoiceStream(conn net.Conn, msg Message) {
	// Parse parameters (recipientID:callSessionID or just recipientID)
	parts := strings.Split(msg.Content, ":")
	if len(parts) < 1 {
		return
	}

	recipientID := parts[0]
	callSessionID := ""
	if len(parts) > 1 {
		callSessionID = parts[1]
	}

	// Look up in local storage
	if n.storage == nil {
		return
	}

	chunks, err := n.storage.GetContentByType(recipientID, storage.TypeVoiceStream)
	if err != nil {
		return
	}

	// Filter by call session if provided
	if callSessionID != "" {
		var filteredChunks []*storage.EncryptedContent
		for _, chunk := range chunks {
			if strings.HasPrefix(chunk.ID, callSessionID) || chunk.ID == callSessionID {
				filteredChunks = append(filteredChunks, chunk)
			}
		}
		chunks = filteredChunks
	}

	// Limit to 10 most recent chunks to prevent flooding
	if len(chunks) > 10 {
		// Sort by timestamp first (newer first)
		sort.Slice(chunks, func(i, j int) bool {
			return chunks[i].Timestamp.After(chunks[j].Timestamp)
		})
		chunks = chunks[:10]
	}

	// Send chunks back
	for _, chunk := range chunks {
		chunkData, err := json.Marshal(chunk)
		if err != nil {
			continue
		}

		response := Message{
			Type:    CmdVoiceStreamResult,
			Sender:  n.address,
			Content: string(chunkData),
		}

		responseData, _ := json.Marshal(response)
		conn.Write(responseData)

		// Brief delay to prevent flooding
		time.Sleep(50 * time.Millisecond)
	}
}

// handleVoiceStreamResult processes voice stream chunk results
func (n *Node) handleVoiceStreamResult(conn net.Conn, msg Message) {
	// Parse chunk data
	var chunk storage.EncryptedContent
	if err := json.Unmarshal([]byte(msg.Content), &chunk); err != nil {
		return
	}

	// Store locally if we have storage
	if n.storage != nil {
		n.storage.StoreContent(&chunk)
	}
}

// handleFindUserInfo handles requests to find user information
func (n *Node) handleFindUserInfo(conn net.Conn, msg Message) {
	username := msg.Content
	userKey := fmt.Sprintf("user:%s", username)

	// Look up in local DHT
	userInfo, found := n.dht.GetValue(userKey)
	if !found {
		// Not found - instead of silently failing, we could forward
		// the request to other nodes we know about
		fmt.Printf("User %s not found locally\n", username)
		return
	}

	// Send result back
	response := Message{
		Type:    CmdUserInfoResult,
		Sender:  n.address,
		Content: fmt.Sprintf("{\"username\":\"%s\",\"info\":%s}", username, userInfo),
	}

	responseData, _ := json.Marshal(response)
	if _, err := conn.Write(responseData); err != nil {
		fmt.Printf("Failed to send user info result: %v\n", err)
	} else {
		fmt.Printf("Sent user info for %s\n", username)
	}
}

// handleUserInfoResult handles user info lookup results
func (n *Node) handleUserInfoResult(conn net.Conn, msg Message) {
	// Parse result
	var result struct {
		Username string `json:"username"`
		Info     string `json:"info"`
	}

	if err := json.Unmarshal([]byte(msg.Content), &result); err != nil {
		fmt.Printf("Invalid user info result: %v\n", err)
		return
	}

	// Store in local DHT
	userKey := fmt.Sprintf("user:%s", result.Username)
	n.dht.StoreValue(userKey, result.Info)

	fmt.Printf("Received user info for %s from %s\n", result.Username, msg.Sender)

	// Check if someone is waiting for this result
	pendingUserMutex.RLock()
	resultChan, exists := pendingUserLookups[result.Username]
	pendingUserMutex.RUnlock()

	if exists {
		// Try to send it to the waiting goroutine
		select {
		case resultChan <- result.Info:
			fmt.Printf("Successfully delivered user info for %s\n", result.Username)
		default:
			// Either the channel is full or the lookup has timed out
			fmt.Printf("Could not deliver user info for %s, channel unavailable\n", result.Username)
		}
	}
}

// handleFindPhoto handles requests to find a photo
func (n *Node) handleFindPhoto(conn net.Conn, msg Message) {
	// Parse parameters
	parts := strings.Split(msg.Content, ":")
	if len(parts) != 3 {
		return
	}

	userID := parts[0]
	photoID := parts[1]
	chunkIndex, err := strconv.Atoi(parts[2])
	if err != nil {
		return
	}

	// Look up in local storage
	if n.storage == nil {
		return
	}

	photos, err := n.storage.GetContentByType(userID, storage.TypePhoto)
	if err != nil {
		return
	}

	// Find matching photo
	for _, photo := range photos {
		if photo.ID == photoID && photo.ChunkIndex == chunkIndex {
			// Found it, send back
			photoData, err := json.Marshal(photo)
			if err != nil {
				return
			}

			response := Message{
				Type:    CmdPhotoResult,
				Sender:  n.address,
				Content: string(photoData),
			}

			responseData, _ := json.Marshal(response)
			conn.Write(responseData)
			return
		}
	}
}

// handlePhotoResult handles photo lookup results
func (n *Node) handlePhotoResult(conn net.Conn, msg Message) {
	// Parse result
	var photo storage.EncryptedContent
	if err := json.Unmarshal([]byte(msg.Content), &photo); err != nil {
		fmt.Printf("Invalid photo result: %v\n", err)
		return
	}

	// Store locally
	if n.storage != nil {
		n.storage.StoreContent(&photo)
		fmt.Printf("Received photo %s chunk %d for user %s from %s\n",
			photo.ID, photo.ChunkIndex, photo.RecipientID, msg.Sender)
	}

}

// GetAllKnownNodes returns a list of all known nodes in the network
// filterType can be "bootstrap", "regular", or "" for all nodes
func (n *Node) GetAllKnownNodes(filterType string, limit int) []string {
	nodes := make(map[string]bool)
	result := []string{}

	// Add ourselves first
	nodes[n.address] = true

	// Add our node type if it matches the filter or no filter
	if filterType == "" || strings.ToLower(filterType) == strings.ToLower(n.nodeType) {
		result = append(result, n.address)
	}

	// Add directly connected peers
	n.peersMutex.RLock()
	for peerAddr := range n.peers {
		if _, exists := nodes[peerAddr]; !exists {
			nodes[peerAddr] = true

			// Check if we should include this node based on filter
			if filterType != "" {
				// Here we need to determine the node type
				// For simplicity, we'll consider nodes with addresses matching bootstrap nodes as bootstrap nodes
				isBootstrap := false
				for _, bootstrapAddr := range n.bootstrapNodes {
					if strings.HasPrefix(peerAddr, strings.Split(bootstrapAddr, ":")[0]) {
						isBootstrap = true
						break
					}
				}

				nodeType := RegularNode
				if isBootstrap {
					nodeType = BootstrapNode
				}

				if strings.ToLower(filterType) != strings.ToLower(nodeType) {
					continue
				}
			}

			result = append(result, peerAddr)
		}
	}
	n.peersMutex.RUnlock()

	// Add nodes from DHT routing table
	dhtPeers := n.dht.GetAllPeers()
	for _, peer := range dhtPeers {
		if _, exists := nodes[peer.Address]; !exists {
			nodes[peer.Address] = true

			// Apply the same filter logic as above
			if filterType != "" {
				isBootstrap := false
				for _, bootstrapAddr := range n.bootstrapNodes {
					if strings.HasPrefix(peer.Address, strings.Split(bootstrapAddr, ":")[0]) {
						isBootstrap = true
						break
					}
				}

				nodeType := RegularNode
				if isBootstrap {
					nodeType = BootstrapNode
				}

				if strings.ToLower(filterType) != strings.ToLower(nodeType) {
					continue
				}
			}

			result = append(result, peer.Address)
		}
	}

	// Apply limit
	if len(result) > limit {
		result = result[:limit]
	}

	return result
}
