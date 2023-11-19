package lruk

import (
	"container/list"
	"fmt"
	"sync"

	. "github.com/pjimming/HermesDB/define"
)

// LRUKReplacer implements the LRU-k replacement policy.
//
// The LRU-k algorithm evicts a frame whose backward k-distance is maximum
// of all frames. Backward k-distance is computed as the difference in time between
// current timestamp and the timestamp of kth previous access.
//
// A frame with less than k historical references is given
// +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
// classical LRU algorithm is used to choose victim.
type LRUKReplacer struct {
	currSize     SizeT
	replacerSize SizeT // max frame number of replacer
	k            SizeT
	mu           sync.RWMutex

	// accessCount < k_; 按首次访问时间戳进行排序，新来的放到表头
	// timestamp: new -> old; 优先驱逐最靠近末尾的、可驱逐的帧
	historyList *list.List
	historyMap  map[string]*list.Element

	// accessCount >= k_; 按倒数第k次访问时间戳排序，更新的放表头
	// timestamp: new -> old; 如果history_list_为空，使用LRU算法，优先驱逐表尾
	cacheList *list.List
	cacheMap  map[string]*list.Element

	// other
	accessCount map[string]SizeT
	isEvictable map[string]bool
}

// New create a lru-k replacer
func New(numFrames, k SizeT) *LRUKReplacer {
	return &LRUKReplacer{
		currSize:     0,
		replacerSize: numFrames,
		k:            k,
		mu:           sync.RWMutex{},
		historyList:  list.New(),
		historyMap:   make(map[string]*list.Element),
		cacheList:    list.New(),
		cacheMap:     make(map[string]*list.Element),
		accessCount:  make(map[string]SizeT),
		isEvictable:  make(map[string]bool),
	}
}

// Evict Find the frame with the largest backward k-distance and evict that frame. Only frames
// that are marked as 'evictable' are candidates for eviction.
//
// A frame with less than k historical references is given +inf as its backward k-distance.
// If multiple frames have inf backward k-distance, then evict the frame with the earliest
// timestamp overall.
//
// Successful eviction of a frame should decrement the size of replacer and remove the frame's
// access history.
func (r *LRUKReplacer) Evict() (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.currSize == 0 {
		return "", false
	}

	for e := r.historyList.Back(); e != nil; e = e.Prev() {
		key, ok := e.Value.(string)
		if !ok {
			panic("value is not string")
		}

		if !r.isEvictable[key] {
			continue
		}

		r.historyList.Remove(e)
		delete(r.historyMap, key)
		r.clearFrame(key)
		return key, true
	}

	for e := r.cacheList.Back(); e != nil; e = e.Prev() {
		key, ok := e.Value.(string)
		if !ok {
			panic("value is not string")
		}

		if !r.isEvictable[key] {
			continue
		}

		r.cacheList.Remove(e)
		delete(r.cacheMap, key)
		r.clearFrame(key)
		return key, true
	}
	return "", false
}

// RecordAccess Record the event that the given frame id is accessed at current timestamp.
// Create a new entry for access history if frame id has not been seen before.
//
// If frame id is invalid (i.e. larger than replacer_size_), throw an exception.
func (r *LRUKReplacer) RecordAccess(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.accessCount[key]++

	if r.accessCount[key] == r.k {
		// history -> cache
		r.historyList.Remove(r.historyMap[key])
		delete(r.historyMap, key)

		r.cacheList.PushFront(key)
		r.cacheMap[key] = r.cacheList.Front()
	} else if r.accessCount[key] > r.k {
		// cache
		if e, ok := r.cacheMap[key]; ok {
			r.cacheList.Remove(e)
		}

		r.cacheList.PushFront(key)
		r.cacheMap[key] = r.cacheList.Front()
	} else {
		// history
		if e, ok := r.historyMap[key]; ok {
			r.historyList.Remove(e)
		}

		r.historyList.PushFront(key)
		r.historyMap[key] = r.historyList.Front()
	}
}

// SetEvictable Toggle whether a frame is evictable or non-evictable. This function also
// controls replacer's size. Note that size is equal to number of evictable entries.
//
// If a frame was previously evictable and is to be set to non-evictable, then size should
// decrement. If a frame was previously non-evictable and is to be set to evictable,
// then size should increment.
//
// If frame id is invalid, throw an exception or abort the process.
//
// For other scenarios, this function should terminate without modifying anything.
func (r *LRUKReplacer) SetEvictable(key string, isEvict bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.accessCount[key]; !ok {
		// not in buffer pool
		return
	}

	sizeChange := 0
	if isEvict != r.isEvictable[key] {
		sizeChange = 1
	}

	if isEvict {
		r.currSize += SizeT(sizeChange)
	} else {
		r.currSize -= SizeT(sizeChange)
	}

	r.isEvictable[key] = isEvict
}

// Remove an evictable frame from replacer, along with its access history.
// This function should also decrement replacer's size if removal is successful.
//
// Note that this is different from evicting a frame, which always remove the frame
// with the largest backward k-distance. This function removes specified frame id,
// no matter what its backward k-distance is.
//
// If Remove is called on a non-evictable frame, throw an exception or abort the
// process.
//
// If specified frame is not found, directly return from this function.
func (r *LRUKReplacer) Remove(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.accessCount[key]; !ok {
		// not in buffer pool
		return
	}

	if !r.isEvictable[key] {
		panic(fmt.Errorf("[Remove] %v can not to remove", key))
	}

	if r.accessCount[key] >= r.k {
		// cache
		r.cacheList.Remove(r.cacheMap[key])
		delete(r.cacheMap, key)
	} else {
		// history
		r.historyList.Remove(r.historyMap[key])
		delete(r.historyMap, key)
	}

	r.clearFrame(key)
}

// Size Return replacer's size, which tracks the number of evictable frames.
func (r *LRUKReplacer) Size() SizeT {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currSize
}

// internal

func (r *LRUKReplacer) clearFrame(key string) {
	delete(r.accessCount, key)
	delete(r.isEvictable, key)
	r.currSize--
}

//func (r *LRUKReplacer) checkOverstep(key string) {
//	if key > string(r.replacerSize) {
//		panic(fmt.Errorf("[RecordAccess] %d uppermore than replacer size", key))
//	}
//}
