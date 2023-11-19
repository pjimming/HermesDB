package extendiblehashtable

import (
	"sync"

	. "github.com/pjimming/HermesDB/define"
	"github.com/pjimming/HermesDB/helper"
)

type ExtendibleHashTable struct {
	globalDepth SizeT
	bucketSize  SizeT
	numBuckets  SizeT
	mu          sync.RWMutex
	dir         []*Bucket
}

func NewExtendibleHashTable(bucketSize SizeT) *ExtendibleHashTable {
	dir := make([]*Bucket, 0)
	dir = append(dir, NewBucket(bucketSize, 0))
	return &ExtendibleHashTable{
		globalDepth: 0,
		bucketSize:  bucketSize,
		numBuckets:  1,
		mu:          sync.RWMutex{},
		dir:         dir,
	}
}

func (e *ExtendibleHashTable) IndexOf(key string) uint32 {
	mask := uint32((1 << e.globalDepth) - 1)
	return helper.Hash32([]byte(key)) & mask
}

func (e *ExtendibleHashTable) Find(key string) (value any, isFind bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	index := e.IndexOf(key)
	targetBucket := e.dir[index]
	return targetBucket.Find(key)
}

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
		if e.GetLocalDepth(e.IndexOf(key)) == e.GetGlobalDepth() {
			e.globalDepth++
			for i := 0; i < len(e.dir); i++ {
				e.dir = append(e.dir, e.dir[i])
			}
		}

		targetBucket := e.dir[e.IndexOf(key)]

		// Increment the local depth of the bucket.
		targetBucket.IncrementDepth()

		// Split the bucket and redistribute directory pointers & the kv pairs in the bucket.
		bucket0 := NewBucket(e.bucketSize, targetBucket.GetDepth())
		bucket1 := NewBucket(e.bucketSize, targetBucket.GetDepth())
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
			entry := item.Value.(*Entry)
			e.dir[e.IndexOf(entry.key)].Insert(entry.key, entry.value)
		}
	}
}

func (e *ExtendibleHashTable) GetGlobalDepth() SizeT {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.globalDepth
}

func (e *ExtendibleHashTable) GetLocalDepth(dirIndex uint32) SizeT {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.dir[dirIndex].GetDepth()
}

func (e *ExtendibleHashTable) GetNumBuckets() SizeT {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.numBuckets
}
