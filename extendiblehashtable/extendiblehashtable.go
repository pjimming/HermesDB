package extendiblehashtable

import (
	"sync"

	. "github.com/pjimming/HermesDB/define"
	"github.com/pjimming/HermesDB/extendiblehashtable/bucket"
	"github.com/pjimming/HermesDB/helper"
)

type ExtendibleHashTable struct {
	globalDepth SizeT
	bucketSize  SizeT
	numBuckets  SizeT
	mu          sync.RWMutex
	dir         []*bucket.Bucket
}

// New create an extendible hash table
func New(bucketSize SizeT) *ExtendibleHashTable {
	dir := make([]*bucket.Bucket, 0)
	dir = append(dir, bucket.NewBucket(bucketSize, 0))
	return &ExtendibleHashTable{
		globalDepth: 0,
		bucketSize:  bucketSize,
		numBuckets:  1,
		mu:          sync.RWMutex{},
		dir:         dir,
	}
}

// IndexOf For the given key, return the entry index in the directory where the key hashes to.
func (e *ExtendibleHashTable) IndexOf(key string) uint32 {
	mask := uint32((1 << e.globalDepth) - 1)
	return helper.Hash32([]byte(key)) & mask
}

// Find the value associated with the given key.
// Use IndexOf(key) to find the directory index the key hashes to.
func (e *ExtendibleHashTable) Find(key string) (value any, isFind bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	index := e.IndexOf(key)
	targetBucket := e.dir[index]
	return targetBucket.Find(key)
}

// Remove Given the key, remove the corresponding key-value pair in the hash table.
func (e *ExtendibleHashTable) Remove(key string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	index := e.IndexOf(key)
	targetBucket := e.dir[index]
	return targetBucket.Remove(key)
}

// Insert the given key-value pair into the hash table.
// If a key already exists, the value should be updated.
// If the bucket is full and can't be inserted, do the following steps before retrying:
//    1. If the local depth of the bucket is equal to the global depth,
//        increment the global depth and double the size of the directory.
//    2. Increment the local depth of the bucket.
//    3. Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
func (e *ExtendibleHashTable) Insert(key string, value any) {
	e.mu.Lock()
	defer e.mu.Unlock()

	for !e.dir[e.IndexOf(key)].Insert(key, value) {
		// If the local depth of the bucket is equal to the global depth,
		// increment the global depth and double the size of the directory.
		if e.getLocalDepth(e.IndexOf(key)) == e.getGlobalDepth() {
			e.globalDepth++
			capacity := len(e.dir)
			for i := 0; i < capacity; i++ {
				e.dir = append(e.dir, e.dir[i])
			}
		}

		targetBucket := e.dir[e.IndexOf(key)]

		// Increment the local depth of the bucket.
		targetBucket.IncrementDepth()

		// Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
		bucket0 := bucket.NewBucket(e.bucketSize, targetBucket.GetDepth())
		bucket1 := bucket.NewBucket(e.bucketSize, targetBucket.GetDepth())
		localDepthMask := uint32(1 << (targetBucket.GetDepth() - 1))
		e.numBuckets++

		// 重新安排指针
		for i := uint32(0); i < uint32(len(e.dir)); i++ {
			if e.dir[i] == targetBucket {
				if i&localDepthMask == 0 {
					e.dir[i] = bucket0
				} else {
					e.dir[i] = bucket1
				}
			}
		}

		// bucket元素重新分配
		for item := targetBucket.GetItems().Front(); item != nil; item = item.Next() {
			entry := item.Value.(*bucket.Entry)
			e.dir[e.IndexOf(entry.GetKey())].Insert(entry.GetKey(), entry.GetValue())
		}
	}
}

// GetGlobalDepth Get the global depth of the directory.
func (e *ExtendibleHashTable) GetGlobalDepth() SizeT {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.getGlobalDepth()
}

// GetLocalDepth Get the local depth of the bucket that the given directory index points to.
func (e *ExtendibleHashTable) GetLocalDepth(dirIndex uint32) SizeT {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.getLocalDepth(dirIndex)
}

// GetNumBuckets Get the number of buckets in the directory.
func (e *ExtendibleHashTable) GetNumBuckets() SizeT {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.getNumBuckets()
}

// internal

func (e *ExtendibleHashTable) getGlobalDepth() SizeT {
	return e.globalDepth
}

func (e *ExtendibleHashTable) getLocalDepth(dirIndex uint32) SizeT {
	return e.dir[dirIndex].GetDepth()
}

func (e *ExtendibleHashTable) getNumBuckets() SizeT {
	return e.numBuckets
}
