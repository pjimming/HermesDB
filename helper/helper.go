package helper

import (
	"hash/fnv"
)

// Hash32 return 32-bit hash value
func Hash32(key []byte) uint32 {
	hasher := fnv.New32()
	_, _ = hasher.Write(key)
	return hasher.Sum32()
}
