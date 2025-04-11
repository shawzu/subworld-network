package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// ContentType defines what kind of content is stored
type ContentType int

const (
	TypeMessage ContentType = iota
	TypePhoto
	TypeVoiceMessage
	TypeCallSignal
	TypeFile
	TypeVoiceStream
	TypeGroupMessage
)

// EncryptedContent represents any content stored in the system
type EncryptedContent struct {
	ID            string      `json:"id"`
	SenderID      string      `json:"sender_id"`
	RecipientID   string      `json:"recipient_id"`
	Type          ContentType `json:"type"`
	EncryptedData string      `json:"encrypted_data"` // Base64 encoded encrypted data
	RawData       []byte      `json:"raw_data,omitempty"`
	Timestamp     time.Time   `json:"timestamp"`
	Delivered     bool        `json:"delivered"`
	Read          bool        `json:"read"`
	Deleted       bool        `json:"deleted"`

	// For chunked content (like photos)
	TotalChunks int `json:"total_chunks,omitempty"`
	ChunkIndex  int `json:"chunk_index,omitempty"`

	// For call signaling
	TTL       int64     `json:"ttl,omitempty"` // Time-to-live in seconds
	ExpiresAt time.Time `json:"expires_at,omitempty"`

	// For files
	FileName string `json:"file_name,omitempty"`
	FileType string `json:"file_type,omitempty"`
	FileSize int64  `json:"file_size,omitempty"`

	// For group messages
	GroupID    string `json:"group_id,omitempty"`
	IsGroupMsg bool   `json:"is_group_msg,omitempty"`
}

// NodeStorage manages persistent storage for a node
type NodeStorage struct {
	db           *leveldb.DB
	dataDir      string
	maxStorageGB int64 // Maximum storage in GB
}

// GroupInfo represents a chat group
type GroupInfo struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	Creator     string    `json:"creator"`
	Members     []string  `json:"members"`
	Admins      []string  `json:"admins"`
	Created     time.Time `json:"created"`
	Updated     time.Time `json:"updated"`
	Avatar      string    `json:"avatar,omitempty"`
}

// NewNodeStorage creates a new storage system
func NewNodeStorage(dataPath string, maxStorageGB int64) (*NodeStorage, error) {
	// Ensure data directory exists
	if err := os.MkdirAll(dataPath, 0755); err != nil {
		return nil, err
	}

	// Open LevelDB database
	db, err := leveldb.OpenFile(filepath.Join(dataPath, "content.db"), nil)
	if err != nil {
		return nil, err
	}

	return &NodeStorage{
		db:           db,
		dataDir:      dataPath,
		maxStorageGB: maxStorageGB,
	}, nil
}

// StoreContent stores encrypted content
func (s *NodeStorage) StoreContent(content *EncryptedContent) error {
	// Set timestamp if not provided
	if content.Timestamp.IsZero() {
		content.Timestamp = time.Now()
	}

	// Ensure we have data in both fields for compatibility
	if len(content.RawData) > 0 && content.EncryptedData == "" {
		content.EncryptedData = string(content.RawData)
	} else if content.EncryptedData != "" && len(content.RawData) == 0 {
		content.RawData = []byte(content.EncryptedData)
	}

	// Set expiry time for signals
	if content.Type == TypeCallSignal && content.TTL > 0 {
		content.ExpiresAt = time.Now().Add(time.Duration(content.TTL) * time.Second)
	}

	// Special handling for voice streams
	if content.Type == TypeVoiceStream {
		// For voice streams, check if we have too many chunks
		recipientChunks, _ := s.GetContentByType(content.RecipientID, TypeVoiceStream)
		if len(recipientChunks) > 1000 { // If we have more than 1000 chunks
			// Sort by age (oldest first)
			sort.Slice(recipientChunks, func(i, j int) bool {
				return recipientChunks[i].Timestamp.Before(recipientChunks[j].Timestamp)
			})

			// Delete the oldest 200 chunks
			chunksToDelete := recipientChunks[:200]
			for _, oldChunk := range chunksToDelete {
				key := fmt.Sprintf("voice:%s:%s:%s", oldChunk.RecipientID, oldChunk.ID, oldChunk.Timestamp.Format(time.RFC3339Nano))
				s.db.Delete([]byte(key), nil)
			}
		}
	}

	// Generate key
	var key string
	switch content.Type {
	case TypeMessage:
		key = fmt.Sprintf("msg:%s:%s:%s", content.RecipientID, time.Now().Format(time.RFC3339Nano), content.ID)
	case TypePhoto:
		key = fmt.Sprintf("photo:%s:%s:%d:%s", content.RecipientID, content.ID, content.ChunkIndex, time.Now().Format(time.RFC3339Nano))
	case TypeVoiceMessage:
		key = fmt.Sprintf("voice:%s:%s:%s", content.RecipientID, time.Now().Format(time.RFC3339Nano), content.ID)
	case TypeCallSignal:
		key = fmt.Sprintf("signal:%s:%s:%s", content.RecipientID, content.ID, time.Now().Format(time.RFC3339Nano))
	case TypeVoiceStream:
		key = fmt.Sprintf("voice:%s:%s:%s", content.RecipientID, content.ID, time.Now().Format(time.RFC3339Nano))
	case TypeFile:
		key = fmt.Sprintf("file:%s:%s:%d:%s", content.RecipientID, content.ID, content.ChunkIndex, time.Now().Format(time.RFC3339Nano))
	}

	// Also store in recipient index
	indexKey := fmt.Sprintf("index:%s:%s", content.RecipientID, content.ID)

	// Serialize and store
	data, err := json.Marshal(content)
	if err != nil {
		return err
	}

	// Store the content
	if err := s.db.Put([]byte(key), data, nil); err != nil {
		return err
	}

	// Store in index (except for call signals which expire quickly)
	// Also don't index voice streams as they're transient
	if content.Type != TypeCallSignal && content.Type != TypeVoiceStream {
		return s.db.Put([]byte(indexKey), []byte(key), nil)
	}

	return nil
}

// GetContent retrieves content by key
func (s *NodeStorage) GetContent(key string) (*EncryptedContent, error) {
	data, err := s.db.Get([]byte(key), nil)
	if err != nil {
		return nil, err
	}

	var content EncryptedContent
	if err := json.Unmarshal(data, &content); err != nil {
		return nil, err
	}

	return &content, nil
}

// GetMessagesByUser retrieves all messages for a user (both direct and group messages)
func (s *NodeStorage) GetMessagesByUser(userID string, includeDeleted bool) ([]*EncryptedContent, error) {
	var messages []*EncryptedContent

	// Get direct messages (existing functionality)
	// Scan the index
	iter := s.db.NewIterator(util.BytesPrefix([]byte(fmt.Sprintf("index:%s:", userID))), nil)
	for iter.Next() {
		// Get the content key from the index
		contentKey := string(iter.Value())

		// Get the actual content
		content, err := s.GetContent(contentKey)
		if err != nil {
			continue
		}

		// Skip deleted messages if not requested
		if !includeDeleted && content.Deleted {
			continue
		}

		messages = append(messages, content)
	}
	iter.Release()

	// Get group messages where the user is a member
	// First, get all groups the user is part of
	groups, err := s.GetUserGroups(userID)
	if err != nil {
		// Log error but continue - we still have direct messages
		fmt.Printf("Warning: Failed to get user groups: %v\n", err)
	} else {
		// For each group, get recent messages (limit to 50 per group to avoid excessive loading)
		for _, group := range groups {
			groupMessages, err := s.GetGroupMessages(group.ID, 50)
			if err != nil {
				// Log error but continue to the next group
				fmt.Printf("Warning: Failed to get messages for group %s: %v\n", group.ID, err)
				continue
			}
			messages = append(messages, groupMessages...)
		}
	}

	// Sort all messages by timestamp
	sort.Slice(messages, func(i, j int) bool {
		return messages[i].Timestamp.After(messages[j].Timestamp)
	})

	return messages, nil
}

// GetContentByType gets all content of a specific type for a user
func (s *NodeStorage) GetContentByType(userID string, contentType ContentType) ([]*EncryptedContent, error) {
	var contents []*EncryptedContent
	var prefix string

	switch contentType {
	case TypeMessage:
		prefix = fmt.Sprintf("msg:%s:", userID)
	case TypePhoto:
		prefix = fmt.Sprintf("photo:%s:", userID)
	case TypeVoiceMessage:
		prefix = fmt.Sprintf("voice:%s:", userID)
	case TypeCallSignal:
		prefix = fmt.Sprintf("signal:%s:", userID)
	}

	iter := s.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	for iter.Next() {
		var content EncryptedContent
		if err := json.Unmarshal(iter.Value(), &content); err != nil {
			continue
		}

		if !content.Deleted {
			contents = append(contents, &content)
		}
	}
	iter.Release()

	return contents, nil
}

// MarkAsDelivered marks content as delivered
func (s *NodeStorage) MarkAsDelivered(userID string, contentIDs []string) error {
	for _, contentID := range contentIDs {
		// Find in index
		indexKey := fmt.Sprintf("index:%s:%s", userID, contentID)
		contentKeyBytes, err := s.db.Get([]byte(indexKey), nil)
		if err != nil {
			continue
		}

		contentKey := string(contentKeyBytes)
		content, err := s.GetContent(contentKey)
		if err != nil {
			continue
		}

		// Mark as delivered
		content.Delivered = true

		// Update
		data, _ := json.Marshal(content)
		s.db.Put([]byte(contentKey), data, nil)
	}

	return nil
}

// DeleteContent marks content as deleted (soft delete)
func (s *NodeStorage) DeleteContent(userID string, contentIDs []string) error {
	for _, contentID := range contentIDs {
		// Find in index
		indexKey := fmt.Sprintf("index:%s:%s", userID, contentID)
		contentKeyBytes, err := s.db.Get([]byte(indexKey), nil)
		if err != nil {
			continue
		}

		contentKey := string(contentKeyBytes)
		content, err := s.GetContent(contentKey)
		if err != nil {
			continue
		}

		// Mark as deleted
		content.Deleted = true

		// Update
		data, _ := json.Marshal(content)
		s.db.Put([]byte(contentKey), data, nil)
	}

	return nil
}

// StoreGroup saves a group to storage
func (s *NodeStorage) StoreGroup(group *GroupInfo) error {
	// Generate key
	key := fmt.Sprintf("group:%s", group.ID)

	// Serialize and store
	data, err := json.Marshal(group)
	if err != nil {
		return fmt.Errorf("failed to marshal group: %w", err)
	}

	// Store the group
	if err := s.db.Put([]byte(key), data, nil); err != nil {
		return fmt.Errorf("failed to store group data: %w", err)
	}

	// Store index entries for each member
	for _, memberID := range group.Members {
		memberKey := fmt.Sprintf("groupmember:%s:%s", memberID, group.ID)
		if err := s.db.Put([]byte(memberKey), []byte(group.ID), nil); err != nil {
			// Log error but continue - secondary indexes are not critical
			fmt.Printf("Warning: Failed to store group member index for %s: %v\n", memberID, err)
		}
	}

	return nil
}

// GetGroup retrieves a group by ID
func (s *NodeStorage) GetGroup(groupID string) (*GroupInfo, error) {
	key := fmt.Sprintf("group:%s", groupID)
	data, err := s.db.Get([]byte(key), nil)
	if err != nil {
		return nil, fmt.Errorf("group not found: %w", err)
	}

	var group GroupInfo
	if err := json.Unmarshal(data, &group); err != nil {
		return nil, fmt.Errorf("failed to unmarshal group data: %w", err)
	}

	return &group, nil
}

// GetUserGroups retrieves all groups a user is a member of
func (s *NodeStorage) GetUserGroups(userID string) ([]*GroupInfo, error) {
	// Use a prefix scan for "groupmember:{userID}:"
	prefix := fmt.Sprintf("groupmember:%s:", userID)
	var groups []*GroupInfo

	iter := s.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	defer iter.Release()

	for iter.Next() {
		// Extract group ID from value
		groupIDBytes := iter.Value()
		groupID := string(groupIDBytes)

		// Get the full group info
		group, err := s.GetGroup(groupID)
		if err != nil {
			// Log error but continue - we want to return as many groups as we can
			fmt.Printf("Warning: Failed to get group %s for user %s: %v\n", groupID, userID, err)
			continue
		}

		groups = append(groups, group)
	}

	if err := iter.Error(); err != nil {
		return groups, fmt.Errorf("error iterating over user groups: %w", err)
	}

	return groups, nil
}

// StoreGroupMessage stores a message for a group
func (s *NodeStorage) StoreGroupMessage(content *EncryptedContent) error {
	// Ensure we have required fields
	if content.GroupID == "" {
		return fmt.Errorf("missing group_id for group message")
	}

	// Set standard message fields
	content.Type = TypeGroupMessage
	content.IsGroupMsg = true

	// Set a timestamp if not provided
	if content.Timestamp.IsZero() {
		content.Timestamp = time.Now()
	}

	// Generate ID if not provided
	if content.ID == "" {
		content.ID = fmt.Sprintf("grpmsg-%d", time.Now().UnixNano())
	}

	// Create a message key (primary storage)
	messageKey := fmt.Sprintf("message:%s:%s:%s", content.GroupID, content.ID, content.Timestamp.Format(time.RFC3339Nano))

	// Serialize the content
	data, err := json.Marshal(content)
	if err != nil {
		return fmt.Errorf("failed to marshal group message: %w", err)
	}

	// Store the message
	if err := s.db.Put([]byte(messageKey), data, nil); err != nil {
		return fmt.Errorf("failed to store group message: %w", err)
	}

	// Store an index for the group message
	groupMsgKey := fmt.Sprintf("groupmsg:%s:%s", content.GroupID, content.ID)
	if err := s.db.Put([]byte(groupMsgKey), []byte(messageKey), nil); err != nil {
		// Log error but continue - secondary indexes are not critical
		fmt.Printf("Warning: Failed to store group message index: %v\n", err)
	}

	return nil
}

// GetGroupMessages retrieves messages for a group
func (s *NodeStorage) GetGroupMessages(groupID string, limit int) ([]*EncryptedContent, error) {
	// Use a prefix scan for "groupmsg:{groupID}:"
	prefix := fmt.Sprintf("groupmsg:%s:", groupID)
	var messages []*EncryptedContent

	iter := s.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	defer iter.Release()

	// Map to track seen message IDs to avoid duplicates
	seen := make(map[string]bool)

	for iter.Next() {
		// Get content key from value
		contentKeyBytes := iter.Value()
		contentKey := string(contentKeyBytes)

		// Get the actual message content
		data, err := s.db.Get([]byte(contentKey), nil)
		if err != nil {
			// Log error but continue - we want to return as many messages as we can
			fmt.Printf("Warning: Failed to get message content for key %s: %v\n", contentKey, err)
			continue
		}

		var content EncryptedContent
		if err := json.Unmarshal(data, &content); err != nil {
			fmt.Printf("Warning: Failed to unmarshal message content: %v\n", err)
			continue
		}

		// Skip deleted messages
		if content.Deleted {
			continue
		}

		// Check for duplicates
		if seen[content.ID] {
			continue
		}

		seen[content.ID] = true
		messages = append(messages, &content)

		// Apply limit if specified
		if limit > 0 && len(messages) >= limit {
			break
		}
	}

	if err := iter.Error(); err != nil {
		return messages, fmt.Errorf("error iterating over group messages: %w", err)
	}

	// Sort messages by timestamp
	sort.Slice(messages, func(i, j int) bool {
		return messages[i].Timestamp.Before(messages[j].Timestamp)
	})

	return messages, nil
}

// AddUserToGroup adds a user to a group
func (s *NodeStorage) AddUserToGroup(groupID, userID string) error {
	// First get the group
	group, err := s.GetGroup(groupID)
	if err != nil {
		return fmt.Errorf("failed to get group to add user: %w", err)
	}

	// Check if user already in the group
	for _, member := range group.Members {
		if member == userID {
			return nil // Already a member, not an error
		}
	}

	// Add the user
	group.Members = append(group.Members, userID)
	group.Updated = time.Now()

	// Update the group
	if err := s.StoreGroup(group); err != nil {
		return fmt.Errorf("failed to update group with new member: %w", err)
	}

	// Also create a member index
	memberKey := fmt.Sprintf("groupmember:%s:%s", userID, groupID)
	if err := s.db.Put([]byte(memberKey), []byte(groupID), nil); err != nil {
		return fmt.Errorf("failed to store group member index: %w", err)
	}

	return nil
}

// RemoveUserFromGroup removes a user from a group
func (s *NodeStorage) RemoveUserFromGroup(groupID, userID string) error {
	// First get the group
	group, err := s.GetGroup(groupID)
	if err != nil {
		return fmt.Errorf("failed to get group to remove user: %w", err)
	}

	// Check if user is in the group
	found := false
	for i, member := range group.Members {
		if member == userID {
			// Remove user from members array
			group.Members = append(group.Members[:i], group.Members[i+1:]...)
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("user %s is not a member of group %s", userID, groupID)
	}

	// Remove user from admins if present
	for i, admin := range group.Admins {
		if admin == userID {
			group.Admins = append(group.Admins[:i], group.Admins[i+1:]...)
			break
		}
	}

	group.Updated = time.Now()

	// Update the group
	if err := s.StoreGroup(group); err != nil {
		return fmt.Errorf("failed to update group after removing member: %w", err)
	}

	// Remove the member index
	memberKey := fmt.Sprintf("groupmember:%s:%s", userID, groupID)
	if err := s.db.Delete([]byte(memberKey), nil); err != nil {
		// Log but continue - this is not critical
		fmt.Printf("Warning: Failed to remove group member index: %v\n", err)
	}

	return nil
}

// IsGroupMember checks if a user is a member of a group
func (s *NodeStorage) IsGroupMember(groupID, userID string) (bool, error) {
	// Try to get from member index first (faster)
	memberKey := fmt.Sprintf("groupmember:%s:%s", userID, groupID)
	_, err := s.db.Get([]byte(memberKey), nil)
	if err == nil {
		// Found the index, user is a member
		return true, nil
	}

	// Index not found, check the full group data as fallback
	group, err := s.GetGroup(groupID)
	if err != nil {
		return false, fmt.Errorf("failed to get group: %w", err)
	}

	// Check membership
	for _, member := range group.Members {
		if member == userID {
			return true, nil
		}
	}

	return false, nil // User is not a member
}

// IsGroupAdmin checks if a user is an admin of a group
func (s *NodeStorage) IsGroupAdmin(groupID, userID string) (bool, error) {
	// Get the group
	group, err := s.GetGroup(groupID)
	if err != nil {
		return false, fmt.Errorf("failed to get group: %w", err)
	}

	// Check admin status
	for _, admin := range group.Admins {
		if admin == userID {
			return true, nil
		}
	}

	return false, nil // User is not an admin
}

// GetGroupMembers returns the list of members for a group
func (s *NodeStorage) GetGroupMembers(groupID string) ([]string, []string, error) {
	// Get the group
	group, err := s.GetGroup(groupID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get group: %w", err)
	}

	return group.Members, group.Admins, nil
}

// DeleteGroup completely removes a group and its member indices
func (s *NodeStorage) DeleteGroup(groupID string) error {
	// First get the group to get the member list
	group, err := s.GetGroup(groupID)
	if err != nil {
		return fmt.Errorf("failed to get group to delete: %w", err)
	}

	// Remove all member indices
	for _, memberID := range group.Members {
		memberKey := fmt.Sprintf("groupmember:%s:%s", memberID, groupID)
		if err := s.db.Delete([]byte(memberKey), nil); err != nil {
			// Log but continue - we want to delete as much as possible
			fmt.Printf("Warning: Failed to delete member index for %s: %v\n", memberID, err)
		}
	}

	// Remove group entry
	groupKey := fmt.Sprintf("group:%s", groupID)
	if err := s.db.Delete([]byte(groupKey), nil); err != nil {
		return fmt.Errorf("failed to delete group: %w", err)
	}

	// Find and delete group messages (would be more efficient with a transaction)
	prefix := fmt.Sprintf("groupmsg:%s:", groupID)
	iter := s.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	defer iter.Release()

	// Collect keys to delete
	var messagesToDelete [][]byte
	var messageIndexesToDelete [][]byte

	for iter.Next() {
		messageIndexKey := iter.Key()
		messageIndexesToDelete = append(messageIndexesToDelete, append([]byte{}, messageIndexKey...))

		messageKey := iter.Value()
		messagesToDelete = append(messagesToDelete, append([]byte{}, messageKey...))
	}

	// Delete message indexes
	for _, key := range messageIndexesToDelete {
		if err := s.db.Delete(key, nil); err != nil {
			fmt.Printf("Warning: Failed to delete message index: %v\n", err)
		}
	}

	// Delete messages
	for _, key := range messagesToDelete {
		if err := s.db.Delete(key, nil); err != nil {
			fmt.Printf("Warning: Failed to delete message: %v\n", err)
		}
	}

	return nil
}

// PermanentlyDeleteContent completely removes content
func (s *NodeStorage) PermanentlyDeleteContent(userID string, contentIDs []string) error {
	for _, contentID := range contentIDs {
		// Find in index
		indexKey := fmt.Sprintf("index:%s:%s", userID, contentID)
		contentKeyBytes, err := s.db.Get([]byte(indexKey), nil)
		if err != nil {
			continue
		}

		contentKey := string(contentKeyBytes)

		// Delete the content and the index
		s.db.Delete([]byte(contentKey), nil)
		s.db.Delete([]byte(indexKey), nil)
	}

	return nil
}

// CleanupExpiredContent removes expired content (like call signals and voice streams)
func (s *NodeStorage) CleanupExpiredContent() error {
	now := time.Now()

	// Find expired call signals
	iter := s.db.NewIterator(util.BytesPrefix([]byte("signal:")), nil)
	for iter.Next() {
		var content EncryptedContent
		if err := json.Unmarshal(iter.Value(), &content); err != nil {
			continue
		}

		// Delete if expired
		if !content.ExpiresAt.IsZero() && content.ExpiresAt.Before(now) {
			s.db.Delete(iter.Key(), nil)
		}
	}
	iter.Release()

	// Find expired voice streams (they expire much faster than other content)
	voiceIter := s.db.NewIterator(util.BytesPrefix([]byte("voice:")), nil)
	for voiceIter.Next() {
		var content EncryptedContent
		if err := json.Unmarshal(voiceIter.Value(), &content); err != nil {
			continue
		}

		// Voice streams expire after 60 seconds by default
		if time.Since(content.Timestamp) > 60*time.Second {
			s.db.Delete(voiceIter.Key(), nil)
		}
	}
	voiceIter.Release()

	return nil
}

// Close closes the storage
func (s *NodeStorage) Close() error {
	return s.db.Close()
}

// GetDataDir returns the data directory
func (s *NodeStorage) GetDataDir() string {
	return s.dataDir
}

// GetMaxStorageGB returns the maximum storage in GB
func (s *NodeStorage) GetMaxStorageGB() int64 {
	return s.maxStorageGB
}

// StoreGroupMemberIndex stores an index mapping a user to a group
func (s *NodeStorage) StoreGroupMemberIndex(userID, groupID string) error {
	memberKey := fmt.Sprintf("groupmember:%s:%s", userID, groupID)
	return s.db.Put([]byte(memberKey), []byte(groupID), nil)
}

// DeleteGroupMemberIndex removes a user-group index
func (s *NodeStorage) DeleteGroupMemberIndex(userID, groupID string) error {
	memberKey := fmt.Sprintf("groupmember:%s:%s", userID, groupID)
	return s.db.Delete([]byte(memberKey), nil)
}

// StoreGroupMessageIndex stores an index for a group message
func (s *NodeStorage) StoreGroupMessageIndex(groupID, messageID string, messageKey string) error {
	groupMsgKey := fmt.Sprintf("groupmsg:%s:%s", groupID, messageID)
	return s.db.Put([]byte(groupMsgKey), []byte(messageKey), nil)
}

// GetRawValue retrieves a raw value from the database
func (s *NodeStorage) GetRawValue(key []byte) ([]byte, error) {
	return s.db.Get(key, nil)
}

// PutRawValue stores a raw value in the database
func (s *NodeStorage) PutRawValue(key, value []byte) error {
	return s.db.Put(key, value, nil)
}

// DeleteRawValue deletes a raw value from the database
func (s *NodeStorage) DeleteRawValue(key []byte) error {
	return s.db.Delete(key, nil)
}
