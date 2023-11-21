package cachemanager

import (
	"sync"

	. "github.com/pjimming/HermesDB/define"
	"github.com/pjimming/HermesDB/extendiblehashtable"
	"github.com/pjimming/HermesDB/lruk"
)

type CacheManager struct {
	table    *extendiblehashtable.ExtendibleHashTable
	replacer *lruk.LRUKReplacer
	mus      map[string]*sync.RWMutex
}

func NewCacheManager(bucketSize, numFrames, k SizeT) *CacheManager {
	return &CacheManager{
		table:    extendiblehashtable.New(bucketSize),
		replacer: lruk.New(numFrames, k),
		mus:      make(map[string]*sync.RWMutex),
	}
}

// Set a pair of k-v data to cache
func (c *CacheManager) Set(key string, value any) {
	c.mutex(key).Lock()
	defer c.mutex(key).Unlock()

	// hash table
	c.table.Insert(key, value)

	// replacer
	c.replacer.RecordAccess(key)
	c.replacer.SetEvictable(key, true)
}

func (c *CacheManager) Get(key string) any {
	c.mutex(key).RLock()
	defer c.mutex(key).RUnlock()

	// hash table
	value, isFind := c.table.Find(key)
	if !isFind {
		return nil
	}

	// replacer
	c.replacer.RecordAccess(key)

	return value
}

func (c *CacheManager) mutex(key string) *sync.RWMutex {
	if _, ok := c.mus[key]; !ok {
		c.mus[key] = &sync.RWMutex{}
	}
	return c.mus[key]
}
