package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"subworld-network/internal/network"
	"subworld-network/internal/storage"
	"time"
)

const CmdFindAllUserContent = "FIND_ALL_USER_CONTENT"

// NodeAPI handles client API requests
type NodeAPI struct {
	node            *network.Node
	storage         *storage.NodeStorage
	apiBindAddress  string
	tls             bool
	tlsCertFile     string
	tlsKeyFile      string
	maxUploadSizeMB int64
}

// APIConfig contains configuration for the API server
type APIConfig struct {
	BindAddress     string
	Port            int
	TLSEnabled      bool
	TLSCertFile     string
	TLSKeyFile      string
	MaxUploadSizeMB int64
}

// NewNodeAPI creates a new API handler
func NewNodeAPI(node *network.Node, storage *storage.NodeStorage, config APIConfig) *NodeAPI {
	// Set default values
	if config.MaxUploadSizeMB == 0 {
		config.MaxUploadSizeMB = 10 // Default 10MB
	}

	bindAddr := fmt.Sprintf("%s:%d", config.BindAddress, config.Port)
	if config.BindAddress == "" {
		bindAddr = fmt.Sprintf(":%d", config.Port)
	}

	return &NodeAPI{
		node:            node,
		storage:         storage,
		apiBindAddress:  bindAddr,
		tls:             config.TLSEnabled,
		tlsCertFile:     config.TLSCertFile,
		tlsKeyFile:      config.TLSKeyFile,
		maxUploadSizeMB: config.MaxUploadSizeMB,
	}
}

// StartServer starts the HTTP API server
func (api *NodeAPI) StartServer() error {
	mux := http.NewServeMux()

	// Message endpoints
	mux.HandleFunc("/messages/send", api.handleSendMessage)
	mux.HandleFunc("/messages/get", api.handleGetMessages)
	mux.HandleFunc("/messages/delivered", api.handleMarkDelivered)
	mux.HandleFunc("/messages/delete", api.handleDeleteMessages)

	// Photo endpoints
	mux.HandleFunc("/photos/upload", api.handleUploadPhoto)
	mux.HandleFunc("/photos/get", api.handleGetPhoto)

	// Call signaling endpoints
	mux.HandleFunc("/calls/signal", api.handleCallSignal)
	mux.HandleFunc("/calls/pending", api.handleGetPendingCallSignals)

	// User endpoints
	mux.HandleFunc("/users/find", api.handleFindUser)
	mux.HandleFunc("/users/register", api.handleRegisterUser)

	// Node information endpoint
	mux.HandleFunc("/node/info", api.handleNodeInfo)

	// Health check
	mux.HandleFunc("/health", api.handleHealthCheck)

	mux.HandleFunc("/nodes/list", api.handleNodesList)

	// CORS middleware
	handler := corsMiddleware(mux)

	// Logging middleware
	handler = loggingMiddleware(handler)

	fmt.Printf("Starting API server on %s\n", api.apiBindAddress)

	if api.tls {
		return http.ListenAndServeTLS(api.apiBindAddress, api.tlsCertFile, api.tlsKeyFile, handler)
	}
	return http.ListenAndServe(api.apiBindAddress, handler)
}

// CORS middleware to allow cross-origin requests
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Logging middleware to log API requests
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		fmt.Printf("[%s] %s %s %s\n", time.Since(start), r.Method, r.URL.Path, r.RemoteAddr)
	})
}

// handleNodesList returns a list of all known nodes in the network
func (api *NodeAPI) handleNodesList(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get the filter query parameter (optional)
	filterType := r.URL.Query().Get("type")
	maxNodes := 100 // Default limit

	// Get max parameter if provided
	maxParam := r.URL.Query().Get("max")
	if maxParam != "" {
		if parsedMax, err := strconv.Atoi(maxParam); err == nil && parsedMax > 0 {
			maxNodes = parsedMax
		}
	}

	// Get all peers from DHT and direct connections
	knownNodes := api.node.GetAllKnownNodes(filterType, maxNodes)

	// Create response
	response := struct {
		Count int      `json:"count"`
		Nodes []string `json:"nodes"`
	}{
		Count: len(knownNodes),
		Nodes: knownNodes,
	}

	// Return the list
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleSendMessage handles a client request to send a message
func (api *NodeAPI) handleSendMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request body
	var content storage.EncryptedContent
	if err := json.NewDecoder(r.Body).Decode(&content); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate required fields
	if content.RecipientID == "" || content.SenderID == "" || content.EncryptedData == "" {
		http.Error(w, "Missing required fields", http.StatusBadRequest)
		return
	}

	// Set content type
	content.Type = storage.TypeMessage

	// Generate ID if not provided
	if content.ID == "" {
		content.ID = fmt.Sprintf("msg-%d", time.Now().UnixNano())
	}

	// Set timestamp if not provided
	if content.Timestamp.IsZero() {
		content.Timestamp = time.Now()
	}

	// Create a DHT storage manager to handle distribution
	storageManager := network.NewDHTStorageManager(api.node, api.storage)

	// Store and distribute the message
	if err := storageManager.StoreContent(&content); err != nil {
		http.Error(w, "Failed to store message: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Try to deliver directly if recipient is connected
	usernamePrefix := "user:"
	if strings.HasPrefix(content.RecipientID, usernamePrefix) {
		username := strings.TrimPrefix(content.RecipientID, usernamePrefix)
		api.node.SendMessageToUser(username, content.EncryptedData)
	}

	// Return success
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "success",
		"id":     content.ID,
	})
}

// handleGetMessages handles a client request to get messages
func (api *NodeAPI) handleGetMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get user ID from query
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "Missing user_id parameter", http.StatusBadRequest)
		return
	}

	// Get optional parameters
	sinceStr := r.URL.Query().Get("since")
	limitStr := r.URL.Query().Get("limit")
	onlyUndeliveredStr := r.URL.Query().Get("undelivered_only")
	fetchRemoteStr := r.URL.Query().Get("fetch_remote")

	var since time.Time
	if sinceStr != "" {
		timestamp, err := strconv.ParseInt(sinceStr, 10, 64)
		if err == nil {
			since = time.Unix(timestamp, 0)
		}
	}

	limit := 100 // Default
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err == nil && parsedLimit > 0 {
			limit = parsedLimit
		}
	}

	onlyUndelivered := false
	if onlyUndeliveredStr == "true" {
		onlyUndelivered = true
	}

	fetchRemote := true // Default to true
	if fetchRemoteStr == "false" {
		fetchRemote = false
	}

	var messages []*storage.EncryptedContent
	var err error

	// Create a DHT storage manager if it doesn't exist
	storageManager := network.NewDHTStorageManager(api.node, api.storage)

	// Try to fetch all content for this user from the network if requested
	if fetchRemote {
		// This will update our local storage with any remote content
		storageManager.FetchAllUserContent(userID)
	}

	// Get messages from local storage (which may have been updated by the network fetch)
	messages, err = api.storage.GetMessagesByUser(userID, false)
	if err != nil {
		http.Error(w, "Failed to get messages: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Filter by timestamp and delivery status
	var filteredMessages []*storage.EncryptedContent
	for _, msg := range messages {
		if msg.Type != storage.TypeMessage {
			continue
		}

		if !since.IsZero() && !msg.Timestamp.After(since) {
			continue
		}

		if onlyUndelivered && msg.Delivered {
			continue
		}

		filteredMessages = append(filteredMessages, msg)
	}

	// Apply limit
	if len(filteredMessages) > limit {
		filteredMessages = filteredMessages[:limit]
	}

	// Return messages
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(filteredMessages)
}

// handleMarkDelivered handles a client request to mark messages as delivered
func (api *NodeAPI) handleMarkDelivered(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request
	var request struct {
		UserID     string   `json:"user_id"`
		MessageIDs []string `json:"message_ids"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if request.UserID == "" || len(request.MessageIDs) == 0 {
		http.Error(w, "Missing required fields", http.StatusBadRequest)
		return
	}

	// Mark messages as delivered
	if err := api.storage.MarkAsDelivered(request.UserID, request.MessageIDs); err != nil {
		http.Error(w, "Failed to mark messages as delivered: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Return success
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "success",
	})
}

// handleDeleteMessages handles a client request to delete messages
func (api *NodeAPI) handleDeleteMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request
	var request struct {
		UserID     string   `json:"user_id"`
		MessageIDs []string `json:"message_ids"`
		Permanent  bool     `json:"permanent"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if request.UserID == "" || len(request.MessageIDs) == 0 {
		http.Error(w, "Missing required fields", http.StatusBadRequest)
		return
	}

	var err error
	if request.Permanent {
		// Permanently delete messages
		err = api.storage.PermanentlyDeleteContent(request.UserID, request.MessageIDs)
	} else {
		// Soft delete messages
		err = api.storage.DeleteContent(request.UserID, request.MessageIDs)
	}

	if err != nil {
		http.Error(w, "Failed to delete messages: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Return success
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "success",
	})
}

// handleUploadPhoto handles a client request to upload a photo
func (api *NodeAPI) handleUploadPhoto(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	fmt.Println("Upload photo request received")

	// Limit upload size
	r.Body = http.MaxBytesReader(w, r.Body, api.maxUploadSizeMB*1024*1024)

	// Parse multipart form
	err := r.ParseMultipartForm(api.maxUploadSizeMB * 1024 * 1024)
	if err != nil {
		fmt.Printf("Failed to parse form: %v\n", err)
		http.Error(w, "Failed to parse form: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Get form values
	recipientID := r.FormValue("recipient_id")
	senderID := r.FormValue("sender_id")
	contentID := r.FormValue("content_id")
	chunkIndexStr := r.FormValue("chunk_index")
	totalChunksStr := r.FormValue("total_chunks")

	fmt.Printf("Photo upload data: recipient=%s, sender=%s, id=%s\n", recipientID, senderID, contentID)

	if recipientID == "" || senderID == "" {
		fmt.Println("Missing required parameters")
		http.Error(w, "Missing required parameters", http.StatusBadRequest)
		return
	}

	// Generate content ID if not provided
	if contentID == "" {
		contentID = fmt.Sprintf("photo-%d", time.Now().UnixNano())
	}

	// Parse chunk info
	chunkIndex := 0
	totalChunks := 1
	if chunkIndexStr != "" {
		chunkIndex, _ = strconv.Atoi(chunkIndexStr)
	}
	if totalChunksStr != "" {
		totalChunks, _ = strconv.Atoi(totalChunksStr)
	}

	// Get file
	file, fileHeader, err := r.FormFile("photo")
	if err != nil {
		fmt.Printf("Failed to get file: %v\n", err)
		http.Error(w, "Failed to get file: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	fmt.Printf("Received file: %s, size: %d bytes\n", fileHeader.Filename, fileHeader.Size)

	// Read file data
	data, err := io.ReadAll(file)
	if err != nil {
		fmt.Printf("Failed to read file: %v\n", err)
		http.Error(w, "Failed to read file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Printf("Read %d bytes of image data\n", len(data))

	// Create content for metadata if first chunk
	if chunkIndex == 0 {
		metaContent := &storage.EncryptedContent{
			ID:            contentID,
			SenderID:      senderID,
			RecipientID:   recipientID,
			Type:          storage.TypePhoto,
			EncryptedData: "", // Metadata only
			Timestamp:     time.Now(),
			TotalChunks:   totalChunks,
			ChunkIndex:    -1, // Special value for metadata
		}

		// Create storage manager
		storageManager := network.NewDHTStorageManager(api.node, api.storage)

		// Store metadata with distribution
		fmt.Println("Storing photo metadata in DHT")
		if err := storageManager.StorePhoto(metaContent); err != nil {
			fmt.Printf("Failed to store photo metadata: %v\n", err)
			http.Error(w, "Failed to store photo metadata: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Create content
	content := &storage.EncryptedContent{
		ID:            contentID,
		SenderID:      senderID,
		RecipientID:   recipientID,
		Type:          storage.TypePhoto,
		EncryptedData: string(data), // Assumes data is already encrypted by client
		Timestamp:     time.Now(),
		ChunkIndex:    chunkIndex,
		TotalChunks:   totalChunks,
	}

	// Create storage manager
	storageManager := network.NewDHTStorageManager(api.node, api.storage)

	// Store the photo chunk with distributed replication
	fmt.Printf("Storing photo chunk %d of %d in DHT\n", chunkIndex, totalChunks)
	if err := storageManager.StorePhoto(content); err != nil {
		fmt.Printf("Failed to store photo: %v\n", err)
		http.Error(w, "Failed to store photo: "+err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Println("Photo stored successfully")

	// Return success
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":      "success",
		"id":          contentID,
		"chunk_index": fmt.Sprintf("%d", chunkIndex),
	})
}

// handleGetPhoto handles a client request to get a photo
func (api *NodeAPI) handleGetPhoto(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get parameters
	userID := r.URL.Query().Get("user_id")
	photoID := r.URL.Query().Get("photo_id")
	chunkStr := r.URL.Query().Get("chunk")

	fmt.Printf("Photo get request: user=%s, photo=%s, chunk=%s\n", userID, photoID, chunkStr)

	if userID == "" || photoID == "" {
		fmt.Println("Missing required parameters")
		http.Error(w, "Missing required parameters", http.StatusBadRequest)
		return
	}

	// Create storage manager
	storageManager := network.NewDHTStorageManager(api.node, api.storage)

	// If chunk is specified, get just that chunk
	if chunkStr != "" {
		chunkIndex, err := strconv.Atoi(chunkStr)
		if err != nil {
			fmt.Printf("Invalid chunk index: %v\n", err)
			http.Error(w, "Invalid chunk index", http.StatusBadRequest)
			return
		}

		// Try to fetch the photo from the network
		fmt.Printf("Fetching photo chunk %d from network\n", chunkIndex)
		photo, err := storageManager.FetchPhoto(userID, photoID, chunkIndex)
		if err != nil {
			fmt.Printf("Chunk not found: %v\n", err)
			http.Error(w, "Chunk not found", http.StatusNotFound)
			return
		}

		fmt.Printf("Found photo chunk %d, size: %d bytes\n", chunkIndex, len(photo.EncryptedData))

		// Return the chunk
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", "inline")
		w.Write([]byte(photo.EncryptedData))
		return
	}

	// If no chunk specified, return metadata
	// Get photo content - try local first
	photos, err := api.storage.GetContentByType(userID, storage.TypePhoto)
	if err != nil {
		http.Error(w, "Failed to get photos: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Find the requested photo metadata
	var photoMeta *storage.EncryptedContent
	for _, photo := range photos {
		if photo.ID == photoID && photo.ChunkIndex == -1 {
			photoMeta = photo
			break
		}
	}

	if photoMeta == nil {
		// Try to find any chunk to get metadata
		for _, photo := range photos {
			if photo.ID == photoID {
				photoMeta = &storage.EncryptedContent{
					ID:          photo.ID,
					SenderID:    photo.SenderID,
					RecipientID: photo.RecipientID,
					Type:        storage.TypePhoto,
					Timestamp:   photo.Timestamp,
					TotalChunks: photo.TotalChunks,
					ChunkIndex:  -1, // Metadata
				}
				break
			}
		}

		if photoMeta == nil {
			// Try to fetch from network
			photo, err := storageManager.FetchPhoto(userID, photoID, -1)
			if err != nil {
				http.Error(w, "Photo not found", http.StatusNotFound)
				return
			}
			photoMeta = photo
		}
	}

	// Return metadata
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(photoMeta)
}

// handleCallSignal handles call signaling
func (api *NodeAPI) handleCallSignal(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request
	var signal storage.EncryptedContent
	if err := json.NewDecoder(r.Body).Decode(&signal); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate required fields
	if signal.RecipientID == "" || signal.SenderID == "" || signal.EncryptedData == "" {
		http.Error(w, "Missing required fields", http.StatusBadRequest)
		return
	}

	// Set type and TTL
	signal.Type = storage.TypeCallSignal
	signal.TTL = 300 // 5 minutes

	// Generate ID if not provided
	if signal.ID == "" {
		signal.ID = fmt.Sprintf("signal-%d", time.Now().UnixNano())
	}

	// Store signal
	if err := api.storage.StoreContent(&signal); err != nil {
		http.Error(w, "Failed to store signal: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Try to deliver directly
	api.node.HandleCallSignal(&signal)

	// Return success
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "success",
		"id":     signal.ID,
	})
}

// handleGetPendingCallSignals gets pending call signals for a user
func (api *NodeAPI) handleGetPendingCallSignals(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get user ID from query
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "Missing user_id parameter", http.StatusBadRequest)
		return
	}

	// Get pending call signals
	signals, err := api.storage.GetContentByType(userID, storage.TypeCallSignal)
	if err != nil {
		http.Error(w, "Failed to get call signals: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Return signals
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(signals)
}

// handleFindUser handles user lookup
func (api *NodeAPI) handleFindUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get username from query
	username := r.URL.Query().Get("username")
	if username == "" {
		http.Error(w, "Missing username parameter", http.StatusBadRequest)
		return
	}

	// Look up user with network-wide search
	userInfo, found := api.node.FindUser(username)
	if !found {
		http.Error(w, "User not found", http.StatusNotFound)
		return
	}

	// Return user info
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(userInfo))
}

// handleRegisterUser handles user registration
func (api *NodeAPI) handleRegisterUser(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request
	var request struct {
		Username  string `json:"username"`
		PublicKey string `json:"public_key"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if request.Username == "" {
		http.Error(w, "Missing username", http.StatusBadRequest)
		return
	}

	// Register user with enhanced distribution, passing the public key
	err := api.node.RegisterUser(request.Username, request.PublicKey)
	if err != nil {
		http.Error(w, "Failed to register user: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Return success
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":   "success",
		"username": request.Username,
	})
}

// handleNodeInfo returns information about this node
func (api *NodeAPI) handleNodeInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get node information
	info := map[string]interface{}{
		"node_type":   api.node.GetNodeType(),
		"address":     api.node.GetAddress(),
		"dht_id":      api.node.GetDHT().LocalID.String(),
		"api_version": "1.0",
		"uptime":      time.Now().Unix(), // In a real implementation, track actual uptime
	}

	// Return info
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}

// handleHealthCheck handles health check requests
func (api *NodeAPI) handleHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ok",
		"time":   time.Now().Format(time.RFC3339),
	})
}
