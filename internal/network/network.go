package network

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Constants for node types and commands
const (
	BootstrapNode = "BOOTSTRAP"
	RegularNode   = "REGULAR"

	CmdJoinNetwork  = "JOIN_NETWORK"
	CmdRequestPeers = "REQUEST_PEERS"
	CmdPeersList    = "PEERS_LIST"
	CmdHandshake    = "HANDSHAKE"
)

// NodeConfig contains the configuration for a node
type NodeConfig struct {
	BootstrapNodes []string // List of bootstrap node IPs
	ForceBootstrap bool     // Force this node to be a bootstrap node
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
}

// NewNode creates a new node
func NewNode(config NodeConfig) (*Node, error) {
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
	address := fmt.Sprintf("%s:8080", publicIP)

	// Determine node type based on IP or forced mode
	nodeType := RegularNode

	if config.ForceBootstrap {
		nodeType = BootstrapNode
		fmt.Println("IMPORTANT: Running in FORCED BOOTSTRAP MODE")
	} else {
		// Check if our IP is in the bootstrap list
		for _, bootstrapIP := range config.BootstrapNodes {
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
	}

	fmt.Printf("Node created: Type=%s, Address=%s\n", node.nodeType, node.address)
	return node, nil
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

// Run starts the node operation
func (n *Node) Run() error {
	if n.isRunning {
		return fmt.Errorf("node is already running")
	}

	// Start TCP listener for all nodes
	listener, err := net.Listen("tcp", ":8080")
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

// connectToBootstrapNodes attempts to connect to all bootstrap nodes
func (n *Node) connectToBootstrapNodes() {
	for _, bootstrapAddr := range n.bootstrapNodes {
		// Skip if this is our own address
		if strings.HasPrefix(bootstrapAddr, n.address) {
			continue
		}

		// Ensure the address has a port
		if !strings.Contains(bootstrapAddr, ":") {
			bootstrapAddr = bootstrapAddr + ":8080"
		}

		// Try to connect
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

		// We only need to connect to one bootstrap node
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

		// Handle the connection in a new goroutine
		go n.handlePeer(conn)
	}
}

// Message represents a structured message between nodes
type Message struct {
	Type    string `json:"type"`
	Sender  string `json:"sender"`
	Content string `json:"content"`
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
		case CmdJoinNetwork:
			n.handleJoinNetwork(conn, msg)
		case CmdRequestPeers:
			n.handleRequestPeers(conn, msg)
		case CmdPeersList:
			n.handlePeersList(msg)
		case CmdHandshake:
			n.handleHandshake(conn, msg)
		default:
			// Regular message, broadcast to other peers
			fmt.Printf("Message from %s: %s\n", msg.Sender, msg.Content)
			n.broadcastMessage(msg)
		}
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

	fmt.Printf("Handshake completed with %s\n", msg.Sender)
}

// sendPeerList sends our list of peers to the specified connection
func (n *Node) sendPeerList(conn net.Conn) {
	// Get the current peer list
	var peerList []string
	n.peersMutex.RLock()
	for peer := range n.peers {
		peerList = append(peerList, peer)
	}
	n.peersMutex.RUnlock()

	// Add ourselves to the list
	peerList = append(peerList, n.address)

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
	n.peersMutex.RLock()
	for peer := range n.peers {
		peerList = append(peerList, peer)
	}
	peerList = append(peerList, n.address)
	n.peersMutex.RUnlock()

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

	fmt.Printf("Connected to peer: %s\n", peerAddr)

	// Handle messages from this peer
	go n.handlePeer(conn)
}

// BroadcastMessage broadcasts a text message to all peers
func (n *Node) BroadcastMessage(text string) {
	msg := Message{
		Type:    "MESSAGE",
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

// startHeartbeat periodically sends peer list updates and removes dead connections
func (n *Node) startHeartbeat() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for n.isRunning {
		select {
		case <-ticker.C:
			// Print connected peers
			n.printConnectedPeers()

			// Request peers list from bootstrap nodes if we're a regular node
			if n.nodeType == RegularNode && len(n.peers) == 0 {
				n.connectToBootstrapNodes()
			}

			// If we're a bootstrap node, broadcast the peer list
			if n.nodeType == BootstrapNode {
				n.broadcastPeerList()
			}
		}
	}
}

// printConnectedPeers displays all connected peers
func (n *Node) printConnectedPeers() {
	n.peersMutex.RLock()
	defer n.peersMutex.RUnlock()

	fmt.Printf("Connected peers (%d):\n", len(n.peers))
	for addr := range n.peers {
		fmt.Printf("  - %s\n", addr)
	}
}
