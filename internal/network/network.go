package network

import (
	"fmt"
	"net"
	"strings"
	"time"
)

type Node struct {
	peers         map[string]net.Conn
	isBootstrap   bool
	bootstrapNode string
}

func NewNode(isBootstrap bool, bootstrapNode string) (*Node, error) {
	return &Node{
		peers:         make(map[string]net.Conn),
		isBootstrap:   isBootstrap,
		bootstrapNode: bootstrapNode,
	}, nil
}

func (n *Node) Start() {
	if n.isBootstrap {
		// Start a listener for incoming connections
		listener, err := net.Listen("tcp", ":8080")
		if err != nil {
			fmt.Println("Failed to start listener:", err)
			return
		}

		go n.acceptConnections(listener)
	} else {
		// Connect to the bootstrap node
		err := n.connectToBootstrapNode()
		if err != nil {
			fmt.Println("Failed to connect to bootstrap node:", err)
			return
		}
	}

	// Periodically print connected peers
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.printConnectedPeers()
		}
	}
}

func (n *Node) connectToBootstrapNode() error {
	conn, err := net.Dial("tcp", n.bootstrapNode)
	if err != nil {
		return err
	}

	n.peers[n.bootstrapNode] = conn
	fmt.Println("Connected to bootstrap node:", n.bootstrapNode)

	// Request a list of peers from the bootstrap node
	_, err = conn.Write([]byte("REQUEST_PEERS"))
	if err != nil {
		return err
	}

	go n.handlePeer(conn)
	return nil
}

func (n *Node) acceptConnections(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Failed to accept connection:", err)
			continue
		}

		// Add the new peer to the list
		n.peers[conn.RemoteAddr().String()] = conn
		fmt.Println("New peer connected:", conn.RemoteAddr().String())

		// Send a handshake to the new peer
		_, err = conn.Write([]byte("HANDSHAKE"))
		if err != nil {
			fmt.Println("Failed to send handshake:", err)
			return
		}

		// Handle messages from the peer
		go n.handlePeer(conn)
	}
}

func (n *Node) handlePeer(conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, 1024)
	for {
		nRead, err := conn.Read(buffer)
		if err != nil {
			fmt.Println("Failed to read from peer:", err)
			delete(n.peers, conn.RemoteAddr().String())
			return
		}

		message := string(buffer[:nRead])
		if message == "REQUEST_PEERS" {
			n.sendPeerList(conn)
		} else if strings.HasPrefix(message, "PEERS:") {
			peerList := strings.Split(message[len("PEERS:"):], "\n")
			n.connectToPeers(peerList)
		} else if message == "HANDSHAKE" {
			// Handle handshake from peer
			n.peers[conn.RemoteAddr().String()] = conn
			fmt.Println("Handshake received from:", conn.RemoteAddr().String())
		} else {
			fmt.Printf("Received message from %s: %s\n", conn.RemoteAddr().String(), message)
			n.BroadcastMessageToPeers(message, conn.RemoteAddr().String())
		}
	}
}

func (n *Node) sendPeerList(conn net.Conn) {
	peers := ""
	for addr := range n.peers {
		if addr != conn.RemoteAddr().String() {
			peers += addr + "\n"
		}
	}

	_, err := conn.Write([]byte("PEERS:" + peers))
	if err != nil {
		fmt.Println("Failed to send peer list:", err)
	}
}

func (n *Node) connectToPeers(peerList []string) {
	for _, addr := range peerList {
		if addr != "" && addr != n.bootstrapNode && n.peers[addr] == nil {
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				fmt.Println("Failed to connect to peer:", addr, err)
				continue
			}
			n.peers[addr] = conn
			fmt.Println("Connected to peer:", addr)

			// Send a handshake to the new peer
			_, err = conn.Write([]byte("HANDSHAKE"))
			if err != nil {
				fmt.Println("Failed to send handshake:", err)
				continue
			}

			go n.handlePeer(conn)
		}
	}
}

func (n *Node) BroadcastMessage(message string) {
	for addr, conn := range n.peers {
		_, err := conn.Write([]byte(message))
		if err != nil {
			fmt.Println("Failed to send message to peer:", addr, err)
		}
	}
}

func (n *Node) BroadcastMessageToPeers(message string, senderAddr string) {
	for addr, conn := range n.peers {
		if addr != senderAddr {
			_, err := conn.Write([]byte(message))
			if err != nil {
				fmt.Println("Failed to send message to peer:", addr, err)
			}
		}
	}
}

func (n *Node) printConnectedPeers() {
	fmt.Println("Connected peers:")
	for addr := range n.peers {
		fmt.Println(" -", addr)
	}
}
