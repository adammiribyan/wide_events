package httpapi

import (
	"crypto/sha256"
	"crypto/subtle"
	"strings"
)

func validBearer(authorization, expected string) bool {
	const prefix = "Bearer "
	if !strings.HasPrefix(authorization, prefix) {
		return false
	}
	candidateDigest := sha256.Sum256([]byte(strings.TrimPrefix(authorization, prefix)))
	expectedDigest := sha256.Sum256([]byte(expected))
	return subtle.ConstantTimeCompare(candidateDigest[:], expectedDigest[:]) == 1
}
