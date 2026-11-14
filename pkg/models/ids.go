package models

import (
	"crypto/rand"
	"encoding/hex"
)

// NewID returns a prefixed random identifier such as "ntf_9f2c…".
func NewID(prefix string) string {
	raw := make([]byte, 12)
	_, _ = rand.Read(raw)

	return prefix + "_" + hex.EncodeToString(raw)
}
