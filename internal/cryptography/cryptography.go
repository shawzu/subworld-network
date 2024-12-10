package cryptography

import "fmt"

// GenerateKeyPair creates a mock private and public key for testing purposes.
func GenerateKeyPair() (string, string) {
	privateKey := "mock_private_key"
	publicKey := "mock_public_key"
	fmt.Println("Keys generated successfully")
	return privateKey, publicKey
}
