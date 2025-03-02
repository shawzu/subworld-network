package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
)

// EncryptedContent represents any content stored in the system
type EncryptedContent struct {
	ID            string      `json:"id"`
	SenderID      string      `json:"sender_id"`
	RecipientID   string      `json:"recipient_id"`
	Type          ContentType `json:"type"`
	EncryptedData string      `json:"encrypted_data"` // Base64 encoded encrypted data
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
}

// NodeStorage manages persistent storage for a node
type NodeStorage struct {
	db           *leveldb.DB
	dataDir      string
	maxStorageGB int64 // Maximum storage in GB
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

	// Set expiry time for signals
	if content.Type == TypeCallSignal && content.TTL > 0 {
		content.ExpiresAt = time.Now().Add(time.Duration(content.TTL) * time.Second)
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
	if content.Type != TypeCallSignal {
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

// GetMessagesByUser retrieves all messages for a user
func (s *NodeStorage) GetMessagesByUser(userID string, includeDeleted bool) ([]*EncryptedContent, error) {
	var messages []*EncryptedContent

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

// CleanupExpiredContent removes expired content (like call signals)
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
