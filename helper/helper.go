package helper

import (
	"hash/fnv"
)

func Hash32(key []byte) uint32 {
	hasher := fnv.New32()
	_, _ = hasher.Write(key)
	return hasher.Sum32()
}
