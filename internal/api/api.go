package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
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

	// User endpoints
	mux.HandleFunc("/users/find", api.handleFindUser)
	mux.HandleFunc("/users/register", api.handleRegisterUser)

	// Node information endpoint
	mux.HandleFunc("/node/info", api.handleNodeInfo)

	// Health check
	mux.HandleFunc("/health", api.handleHealthCheck)

	mux.HandleFunc("/nodes/list", api.handleNodesList)

	mux.HandleFunc("/files/upload", api.handleFileUpload)
	mux.HandleFunc("/files/get", api.handleGetFile)

	mux.HandleFunc("/voice/start", api.handleStartVoiceCall)
	mux.HandleFunc("/voice/stream", api.handleVoiceStream)
	mux.HandleFunc("/voice/fetch", api.handleFetchVoiceStream)
	mux.HandleFunc("/voice/end", api.handleEndVoiceCall)

	mux.HandleFunc("/groups/create", api.handleCreateGroup)
	mux.HandleFunc("/groups/get", api.handleGetGroup)
	mux.HandleFunc("/groups/list", api.handleListGroups)
	mux.HandleFunc("/groups/join", api.handleJoinGroup)
	mux.HandleFunc("/groups/leave", api.handleLeaveGroup)
	mux.HandleFunc("/groups/members", api.handleGroupMembers)
	mux.HandleFunc("/groups/messages/send", api.handleSendGroupMessage)
	mux.HandleFunc("/groups/messages/get", api.handleGetGroupMessages)

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

// Handler for creating a new group
func (api *NodeAPI) handleCreateGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request
	var request struct {
		Name        string   `json:"name"`
		Description string   `json:"description"`
		Creator     string   `json:"creator"`          // The user's public key
		Members     []string `json:"members"`          // Initial members (optional)
		Avatar      string   `json:"avatar,omitempty"` // Optional avatar (file ID)
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate required fields
	if request.Name == "" || request.Creator == "" {
		http.Error(w, "Missing required fields", http.StatusBadRequest)
		return
	}

	// Create the group
	group := &storage.GroupInfo{
		ID:          fmt.Sprintf("group-%d", time.Now().UnixNano()),
		Name:        request.Name,
		Description: request.Description,
		Creator:     request.Creator,
		Members:     append([]string{request.Creator}, request.Members...),
		Admins:      []string{request.Creator}, // Creator is always an admin
		Created:     time.Now(),
		Updated:     time.Now(),
		Avatar:      request.Avatar,
	}

	// Store the group
	if err := api.storage.StoreGroup(group); err != nil {
		http.Error(w, "Failed to store group: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Also add group membership indexes for all members
	for _, member := range group.Members {
		api.storage.StoreGroupMemberIndex(member, group.ID)
	}

	// Create a DHT storage manager to handle distribution
	storageManager := network.NewDHTStorageManager(api.node, api.storage)

	// Store the group info in the DHT so other nodes can discover it
	groupKey := fmt.Sprintf("group:%s", group.ID)
	groupData, _ := json.Marshal(group)
	api.node.StoreDHTValue(groupKey, string(groupData))

	// Distribute the group info to appropriate nodes
	storageManager.StoreGroupInfo(group)

	// Return success with group ID
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "success",
		"id":     group.ID,
	})
}

// Handler for getting a specific group
func (api *NodeAPI) handleGetGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get group ID from query
	groupID := r.URL.Query().Get("group_id")
	if groupID == "" {
		http.Error(w, "Missing group_id parameter", http.StatusBadRequest)
		return
	}

	// Create a DHT storage manager for network operations
	storageManager := network.NewDHTStorageManager(api.node, api.storage)

	// Try to fetch group from network first
	group, err := storageManager.FetchGroupInfo(groupID)
	if err != nil {
		// Fall back to local storage
		group, err = api.storage.GetGroup(groupID)
		if err != nil {
			http.Error(w, "Group not found", http.StatusNotFound)
			return
		}
	}

	// Return the group info
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(group)
}

// Handler for listing user's groups
func (api *NodeAPI) handleListGroups(w http.ResponseWriter, r *http.Request) {
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

	// Create a DHT storage manager
	storageManager := network.NewDHTStorageManager(api.node, api.storage)

	// Try to fetch all groups for this user from the network
	storageManager.FetchUserGroups(userID)

	// Get the groups from local storage (which may have been updated by network fetch)
	groups, err := api.storage.GetUserGroups(userID)
	if err != nil {
		http.Error(w, "Failed to get groups: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Return the groups
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(groups)
}

// Handler for joining a group
func (api *NodeAPI) handleJoinGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request
	var request struct {
		GroupID string `json:"group_id"`
		UserID  string `json:"user_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if request.GroupID == "" || request.UserID == "" {
		http.Error(w, "Missing required fields", http.StatusBadRequest)
		return
	}

	// Create a DHT storage manager
	storageManager := network.NewDHTStorageManager(api.node, api.storage)

	// Try to fetch the group first to ensure we have the latest version
	_, err := storageManager.FetchGroupInfo(request.GroupID)
	if err != nil {
		http.Error(w, "Group not found", http.StatusNotFound)
		return
	}

	// Add the user to the group
	if err := api.storage.AddUserToGroup(request.GroupID, request.UserID); err != nil {
		http.Error(w, "Failed to add user to group: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Get the updated group and distribute to the network
	updatedGroup, _ := api.storage.GetGroup(request.GroupID)
	storageManager.StoreGroupInfo(updatedGroup)

	// Return success
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "success",
	})
}

// Handler for leaving a group
func (api *NodeAPI) handleLeaveGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request
	var request struct {
		GroupID string `json:"group_id"`
		UserID  string `json:"user_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if request.GroupID == "" || request.UserID == "" {
		http.Error(w, "Missing required fields", http.StatusBadRequest)
		return
	}

	// Remove the user from the group
	if err := api.storage.RemoveUserFromGroup(request.GroupID, request.UserID); err != nil {
		http.Error(w, "Failed to remove user from group: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Get the updated group and distribute to the network
	storageManager := network.NewDHTStorageManager(api.node, api.storage)
	updatedGroup, _ := api.storage.GetGroup(request.GroupID)
	storageManager.StoreGroupInfo(updatedGroup)

	// Return success
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "success",
	})
}

// Handler for getting group members
func (api *NodeAPI) handleGroupMembers(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get group ID from query
	groupID := r.URL.Query().Get("group_id")
	if groupID == "" {
		http.Error(w, "Missing group_id parameter", http.StatusBadRequest)
		return
	}

	// Get the group
	group, err := api.storage.GetGroup(groupID)
	if err != nil {
		http.Error(w, "Group not found", http.StatusNotFound)
		return
	}

	// Return the members
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"group_id": groupID,
		"members":  group.Members,
		"admins":   group.Admins,
	})
}

// Handler for sending a message to a group
func (api *NodeAPI) handleSendGroupMessage(w http.ResponseWriter, r *http.Request) {
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
	if content.GroupID == "" || content.SenderID == "" || content.EncryptedData == "" {
		http.Error(w, "Missing required fields", http.StatusBadRequest)
		return
	}

	// Set content type and group flag
	content.Type = storage.TypeGroupMessage
	content.IsGroupMsg = true

	// Generate ID if not provided
	if content.ID == "" {
		content.ID = fmt.Sprintf("grpmsg-%d", time.Now().UnixNano())
	}

	// Set timestamp if not provided
	if content.Timestamp.IsZero() {
		content.Timestamp = time.Now()
	}

	// Get the group to verify membership and distribute to members
	group, err := api.storage.GetGroup(content.GroupID)
	if err != nil {
		http.Error(w, "Group not found", http.StatusNotFound)
		return
	}

	// Verify the sender is a member of the group
	isMember := false
	for _, member := range group.Members {
		if member == content.SenderID {
			isMember = true
			break
		}
	}

	if !isMember {
		http.Error(w, "Sender is not a member of the group", http.StatusForbidden)
		return
	}

	// Create a DHT storage manager to handle distribution
	storageManager := network.NewDHTStorageManager(api.node, api.storage)

	// Store the message
	err = storageManager.StoreGroupMessage(&content, group.Members)
	if err != nil {
		http.Error(w, "Failed to store message: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Return success
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "success",
		"id":     content.ID,
	})
}

// Handler for getting group messages
func (api *NodeAPI) handleGetGroupMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get parameters
	groupID := r.URL.Query().Get("group_id")
	if groupID == "" {
		http.Error(w, "Missing group_id parameter", http.StatusBadRequest)
		return
	}

	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		http.Error(w, "Missing user_id parameter", http.StatusBadRequest)
		return
	}

	// Get the group to verify membership
	group, err := api.storage.GetGroup(groupID)
	if err != nil {
		http.Error(w, "Group not found", http.StatusNotFound)
		return
	}

	// Verify the requester is a member of the group
	isMember := false
	for _, member := range group.Members {
		if member == userID {
			isMember = true
			break
		}
	}

	if !isMember {
		http.Error(w, "User is not a member of the group", http.StatusForbidden)
		return
	}

	// Get optional parameters
	sinceStr := r.URL.Query().Get("since")
	limitStr := r.URL.Query().Get("limit")

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

	// Create a DHT storage manager
	storageManager := network.NewDHTStorageManager(api.node, api.storage)

	// Try to fetch messages from the network
	storageManager.FetchGroupMessages(groupID)

	// Get messages from local storage (which may have been updated by the network fetch)
	messages, err := api.storage.GetGroupMessages(groupID, limit)
	if err != nil {
		http.Error(w, "Failed to get messages: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Filter by timestamp
	if !since.IsZero() {
		var filteredMessages []*storage.EncryptedContent
		for _, msg := range messages {
			if msg.Timestamp.After(since) {
				filteredMessages = append(filteredMessages, msg)
			}
		}
		messages = filteredMessages
	}

	// Return messages
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(messages)
}

// Logging middleware to log API requests
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		fmt.Printf("[%s] %s %s %s\n", time.Since(start), r.Method, r.URL.Path, r.RemoteAddr)
	})
}

// handleStartVoiceCall initiates a voice call session
func (api *NodeAPI) handleStartVoiceCall(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request body
	var request struct {
		CallerId      string `json:"caller_id"`
		RecipientId   string `json:"recipient_id"`
		CallSessionId string `json:"call_session_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate required fields
	if request.CallerId == "" || request.RecipientId == "" {
		http.Error(w, "Missing required fields", http.StatusBadRequest)
		return
	}

	// Generate call session ID if not provided
	callSessionId := request.CallSessionId
	if callSessionId == "" {
		callSessionId = fmt.Sprintf("call-%s-%d", request.CallerId, time.Now().UnixNano())
	}

	// Create a call start notification
	notification := &storage.EncryptedContent{
		ID:            callSessionId,
		SenderID:      request.CallerId,
		RecipientID:   request.RecipientId,
		Type:          storage.TypeVoiceStream,
		EncryptedData: "{\"type\":\"call_start\",\"timestamp\":" + strconv.FormatInt(time.Now().Unix(), 10) + "}",
		Timestamp:     time.Now(),
		TTL:           300, // 5 minutes TTL
	}

	// Store notification
	if err := api.storage.StoreContent(notification); err != nil {
		http.Error(w, "Failed to store call notification: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Create a DHT storage manager to handle distribution
	storageManager := network.NewDHTStorageManager(api.node, api.storage)
	storageManager.StoreContent(notification)

	// Return success with call session ID
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":          "success",
		"call_session_id": callSessionId,
	})
}

// handleVoiceStream handles voice data stream chunks
func (api *NodeAPI) handleVoiceStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Limit upload size
	r.Body = http.MaxBytesReader(w, r.Body, 64*1024) // Limit to 64KB per chunk

	// Parse request body
	var request struct {
		CallSessionId string `json:"call_session_id"`
		SenderId      string `json:"sender_id"`
		RecipientId   string `json:"recipient_id"`
		ChunkId       string `json:"chunk_id"`
		AudioData     string `json:"audio_data"` // Base64 encoded encrypted audio
		Timestamp     int64  `json:"timestamp"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate required fields
	if request.CallSessionId == "" || request.SenderId == "" ||
		request.RecipientId == "" || request.AudioData == "" {
		http.Error(w, "Missing required fields", http.StatusBadRequest)
		return
	}

	// Generate chunk ID if not provided
	chunkId := request.ChunkId
	if chunkId == "" {
		chunkId = fmt.Sprintf("%s-%d", request.CallSessionId, time.Now().UnixNano())
	}

	// Create voice chunk content
	chunk := &storage.EncryptedContent{
		ID:            chunkId,
		SenderID:      request.SenderId,
		RecipientID:   request.RecipientId,
		Type:          storage.TypeVoiceStream,
		EncryptedData: request.AudioData,
		Timestamp:     time.Now(),
		TTL:           60, // 60 seconds TTL for audio chunks
	}

	// Store chunk locally
	if err := api.storage.StoreContent(chunk); err != nil {
		http.Error(w, "Failed to store voice chunk: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Create a DHT storage manager to handle distribution
	storageManager := network.NewDHTStorageManager(api.node, api.storage)
	storageManager.StoreContent(chunk)

	// Return success
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":   "success",
		"chunk_id": chunkId,
	})
}

// handleFetchVoiceStream fetches voice stream chunks for a recipient
func (api *NodeAPI) handleFetchVoiceStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get query parameters
	recipientId := r.URL.Query().Get("recipient_id")
	callSessionId := r.URL.Query().Get("call_session_id")
	sinceTimestamp := r.URL.Query().Get("since_timestamp")

	// Validate required parameters
	if recipientId == "" {
		http.Error(w, "Missing recipient_id parameter", http.StatusBadRequest)
		return
	}

	// Default to fetching all chunks for this recipient if call session not specified
	var chunks []*storage.EncryptedContent
	var err error

	// Create a DHT storage manager
	storageManager := network.NewDHTStorageManager(api.node, api.storage)

	// Try to fetch all content for this user from the network
	storageManager.FetchAllUserContent(recipientId)

	// Get voice stream chunks from local storage (might have been updated by network fetch)
	chunks, err = api.storage.GetContentByType(recipientId, storage.TypeVoiceStream)
	if err != nil {
		http.Error(w, "Failed to get voice chunks: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Filter by call session if specified
	if callSessionId != "" {
		var filteredChunks []*storage.EncryptedContent
		for _, chunk := range chunks {
			// Check if the chunk belongs to this call session
			// We assume the call session ID is at the beginning of the chunk ID
			if strings.HasPrefix(chunk.ID, callSessionId) || chunk.ID == callSessionId {
				filteredChunks = append(filteredChunks, chunk)
			}
		}
		chunks = filteredChunks
	}

	// Filter by timestamp if specified
	if sinceTimestamp != "" {
		timestamp, err := strconv.ParseInt(sinceTimestamp, 10, 64)
		if err == nil {
			since := time.Unix(timestamp, 0)
			var filteredChunks []*storage.EncryptedContent
			for _, chunk := range chunks {
				if chunk.Timestamp.After(since) {
					filteredChunks = append(filteredChunks, chunk)
				}
			}
			chunks = filteredChunks
		}
	}

	// Sort chunks by timestamp (newer first)
	sort.Slice(chunks, func(i, j int) bool {
		return chunks[i].Timestamp.After(chunks[j].Timestamp)
	})

	// Limit to 50 most recent chunks to prevent excessive response size
	if len(chunks) > 50 {
		chunks = chunks[:50]
	}

	// Return chunks
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(chunks)
}

// handleEndVoiceCall ends a voice call session
func (api *NodeAPI) handleEndVoiceCall(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse request body
	var request struct {
		CallSessionId string `json:"call_session_id"`
		SenderId      string `json:"sender_id"`
		RecipientId   string `json:"recipient_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate required fields
	if request.CallSessionId == "" || request.SenderId == "" || request.RecipientId == "" {
		http.Error(w, "Missing required fields", http.StatusBadRequest)
		return
	}

	// Create call end notification
	notification := &storage.EncryptedContent{
		ID:            fmt.Sprintf("%s-end", request.CallSessionId),
		SenderID:      request.SenderId,
		RecipientID:   request.RecipientId,
		Type:          storage.TypeVoiceStream,
		EncryptedData: "{\"type\":\"call_end\",\"timestamp\":" + strconv.FormatInt(time.Now().Unix(), 10) + "}",
		Timestamp:     time.Now(),
		TTL:           300, // 5 minutes TTL
	}

	// Store notification
	if err := api.storage.StoreContent(notification); err != nil {
		http.Error(w, "Failed to store call end notification: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Create a DHT storage manager to handle distribution
	storageManager := network.NewDHTStorageManager(api.node, api.storage)
	storageManager.StoreContent(notification)

	// Return success
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "success",
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

// handleFileUpload handles a client request to upload a file
func (api *NodeAPI) handleFileUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	fmt.Println("File upload request received")

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
	fileName := r.FormValue("file_name")
	fileType := r.FormValue("file_type")
	chunkIndexStr := r.FormValue("chunk_index")
	totalChunksStr := r.FormValue("total_chunks")

	fmt.Printf("File upload data: recipient=%s, sender=%s, id=%s, name=%s\n",
		recipientID, senderID, contentID, fileName)

	if recipientID == "" || senderID == "" {
		fmt.Println("Missing required parameters")
		http.Error(w, "Missing required parameters", http.StatusBadRequest)
		return
	}

	// Generate content ID if not provided
	if contentID == "" {
		contentID = fmt.Sprintf("file-%d", time.Now().UnixNano())
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
	file, fileHeader, err := r.FormFile("file")
	if err != nil {
		fmt.Printf("Failed to get file: %v\n", err)
		http.Error(w, "Failed to get file: "+err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	fmt.Printf("Received file: %s, size: %d bytes\n", fileHeader.Filename, fileHeader.Size)

	data, err := io.ReadAll(file)
	if err != nil {
		fmt.Printf("Failed to read file: %v\n", err)
		http.Error(w, "Failed to read file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Printf("Read %d bytes of file data\n", len(data))

	// Create content for metadata if first chunk
	if chunkIndex == 0 {
		metaContent := &storage.EncryptedContent{
			ID:            contentID,
			SenderID:      senderID,
			RecipientID:   recipientID,
			Type:          storage.TypeFile, // New type for files
			EncryptedData: "",               // Metadata only
			Timestamp:     time.Now(),
			TotalChunks:   totalChunks,
			ChunkIndex:    -1, // Special value for metadata
			FileName:      fileName,
			FileType:      fileType,
			FileSize:      fileHeader.Size,
		}

		// Create storage manager
		storageManager := network.NewDHTStorageManager(api.node, api.storage)

		// Store metadata with distribution
		fmt.Println("Storing file metadata in DHT")
		if err := storageManager.StoreFile(metaContent); err != nil {
			fmt.Printf("Failed to store file metadata: %v\n", err)
			http.Error(w, "Failed to store file metadata: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	// Create content
	content := &storage.EncryptedContent{
		ID:          contentID,
		SenderID:    senderID,
		RecipientID: recipientID,
		Type:        storage.TypeFile,
		// Store as string for backward compatibility
		EncryptedData: string(data),
		// Also store as raw bytes
		RawData:     data,
		Timestamp:   time.Now(),
		ChunkIndex:  chunkIndex,
		TotalChunks: totalChunks,
		FileName:    fileName,
		FileType:    fileType,
		FileSize:    fileHeader.Size,
	}
	// Create storage manager
	storageManager := network.NewDHTStorageManager(api.node, api.storage)

	// Store the file chunk with distributed replication
	fmt.Printf("Storing file chunk %d of %d in DHT\n", chunkIndex, totalChunks)
	if err := storageManager.StoreFile(content); err != nil {
		fmt.Printf("Failed to store file: %v\n", err)
		http.Error(w, "Failed to store file: "+err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Println("File stored successfully")

	// Return success
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":      "success",
		"id":          contentID,
		"chunk_index": fmt.Sprintf("%d", chunkIndex),
	})
}

// handleGetFile handles a client request to retrieve a file
func (api *NodeAPI) handleGetFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get parameters
	userID := r.URL.Query().Get("user_id")
	fileID := r.URL.Query().Get("file_id")
	chunkStr := r.URL.Query().Get("chunk")

	fmt.Printf("File get request: user=%s, file=%s, chunk=%s\n", userID, fileID, chunkStr)

	if userID == "" || fileID == "" {
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

		// Try to fetch the file from the network
		fmt.Printf("Fetching file chunk %d from network\n", chunkIndex)
		file, err := storageManager.FetchFile(userID, fileID, chunkIndex)
		if err != nil {
			fmt.Printf("Chunk not found: %v\n", err)
			http.Error(w, "Chunk not found", http.StatusNotFound)
			return
		}

		// Check and log data sizes
		fmt.Printf("Found file chunk %d, rawData size: %d bytes, encryptedData size: %d bytes\n",
			chunkIndex, len(file.RawData), len(file.EncryptedData))

		// Set content type if available
		if file.FileType != "" {
			w.Header().Set("Content-Type", file.FileType)
		} else {
			w.Header().Set("Content-Type", "application/octet-stream")
		}

		// Set filename for download
		if file.FileName != "" {
			w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, file.FileName))
		} else {
			w.Header().Set("Content-Disposition", "attachment")
		}

		// Send data - try RawData first, fall back to EncryptedData
		if len(file.RawData) > 0 {
			fmt.Printf("Writing %d bytes of RawData\n", len(file.RawData))
			w.Write(file.RawData)
		} else if file.EncryptedData != "" {
			fmt.Printf("Writing %d bytes from EncryptedData\n", len(file.EncryptedData))
			w.Write([]byte(file.EncryptedData))
		} else {
			fmt.Println("WARNING: No data available for file chunk")
			http.Error(w, "File chunk has no data", http.StatusInternalServerError)
			return
		}
		return
	}

	// If no chunk specified, return metadata
	// Get file content - try local first
	files, err := api.storage.GetContentByType(userID, storage.TypeFile)
	if err != nil {
		http.Error(w, "Failed to get files: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Find the requested file metadata
	var fileMeta *storage.EncryptedContent
	for _, content := range files {
		if content.ID == fileID && content.ChunkIndex == -1 {
			// This is the metadata
			fileMeta = content
			break
		}
	}

	if fileMeta == nil {
		// Try to find any chunk to get metadata
		for _, content := range files {
			if content.ID == fileID {
				fileMeta = &storage.EncryptedContent{
					ID:          content.ID,
					SenderID:    content.SenderID,
					RecipientID: content.RecipientID,
					Type:        storage.TypeFile,
					Timestamp:   content.Timestamp,
					TotalChunks: content.TotalChunks,
					ChunkIndex:  -1, // Metadata
					FileName:    content.FileName,
					FileType:    content.FileType,
					FileSize:    content.FileSize,
				}
				break
			}
		}

		if fileMeta == nil {
			// Try to fetch from network
			file, err := storageManager.FetchFile(userID, fileID, -1)
			if err != nil {
				http.Error(w, "File not found", http.StatusNotFound)
				return
			}
			fileMeta = file
		}
	}

	// Return metadata
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(fileMeta)
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
