package db

import (
	"crypto/sha256"
	"encoding/hex"
)

func hash(b []byte) string {
	hash := sha256.New224()
	hash.Write(b)
	return hex.EncodeToString(hash.Sum(nil))
}
