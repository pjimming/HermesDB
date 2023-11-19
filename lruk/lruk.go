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
	historyMap  map[FrameIdT]*list.Element

	// accessCount >= k_; 按倒数第k次访问时间戳排序，更新的放表头
	// timestamp: new -> old; 如果history_list_为空，使用LRU算法，优先驱逐表尾
	cacheList *list.List
	cacheMap  map[FrameIdT]*list.Element

	// other
	accessCount map[FrameIdT]SizeT
	isEvictable map[FrameIdT]bool
}

// NewLRUKReplacer create a lru-k replacer
func NewLRUKReplacer(numFrames, k SizeT) *LRUKReplacer {
	return &LRUKReplacer{
		currSize:     0,
		replacerSize: numFrames,
		k:            k,
		mu:           sync.RWMutex{},
		historyList:  list.New(),
		historyMap:   make(map[FrameIdT]*list.Element),
		cacheList:    list.New(),
		cacheMap:     make(map[FrameIdT]*list.Element),
		accessCount:  make(map[FrameIdT]SizeT),
		isEvictable:  make(map[FrameIdT]bool),
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
func (r *LRUKReplacer) Evict() (FrameIdT, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.currSize == 0 {
		return 0, false
	}

	for e := r.historyList.Back(); e != nil; e = e.Prev() {
		frameId, ok := e.Value.(FrameIdT)
		if !ok {
			panic("value is not FrameIdT")
		}

		if !r.isEvictable[frameId] {
			continue
		}

		r.historyList.Remove(e)
		delete(r.historyMap, frameId)
		r.clearFrame(frameId)
		return frameId, true
	}

	for e := r.cacheList.Back(); e != nil; e = e.Prev() {
		frameId, ok := e.Value.(FrameIdT)
		if !ok {
			panic("value is not FrameIdT")
		}

		if !r.isEvictable[frameId] {
			continue
		}

		r.cacheList.Remove(e)
		delete(r.cacheMap, frameId)
		r.clearFrame(frameId)
		return frameId, true
	}
	return 0, false
}

// RecordAccess Record the event that the given frame id is accessed at current timestamp.
// Create a new entry for access history if frame id has not been seen before.
//
// If frame id is invalid (i.e. larger than replacer_size_), throw an exception.
func (r *LRUKReplacer) RecordAccess(frameId FrameIdT) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.checkOverstep(frameId)

	r.accessCount[frameId]++

	if r.accessCount[frameId] == r.k {
		// history -> cache
		r.historyList.Remove(r.historyMap[frameId])
		delete(r.historyMap, frameId)

		r.cacheList.PushFront(frameId)
		r.cacheMap[frameId] = r.cacheList.Front()
	} else if r.accessCount[frameId] > r.k {
		// cache
		if e, ok := r.cacheMap[frameId]; ok {
			r.cacheList.Remove(e)
		}

		r.cacheList.PushFront(frameId)
		r.cacheMap[frameId] = r.cacheList.Front()
	} else {
		// history
		if e, ok := r.historyMap[frameId]; ok {
			r.historyList.Remove(e)
		}

		r.historyList.PushFront(frameId)
		r.historyMap[frameId] = r.historyList.Front()
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
func (r *LRUKReplacer) SetEvictable(frameId FrameIdT, isEvict bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.checkOverstep(frameId)

	if _, ok := r.accessCount[frameId]; !ok {
		// not in buffer pool
		return
	}

	sizeChange := 0
	if isEvict != r.isEvictable[frameId] {
		sizeChange = 1
	}

	if isEvict {
		r.currSize += SizeT(sizeChange)
	} else {
		r.currSize -= SizeT(sizeChange)
	}

	r.isEvictable[frameId] = isEvict
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
func (r *LRUKReplacer) Remove(frameId FrameIdT) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.checkOverstep(frameId)

	if _, ok := r.accessCount[frameId]; !ok {
		// not in buffer pool
		return
	}

	if !r.isEvictable[frameId] {
		panic(fmt.Errorf("[Remove] %d can not to remove", frameId))
	}

	if r.accessCount[frameId] >= r.k {
		// cache
		r.cacheList.Remove(r.cacheMap[frameId])
		delete(r.cacheMap, frameId)
	} else {
		// history
		r.historyList.Remove(r.historyMap[frameId])
		delete(r.historyMap, frameId)
	}

	r.clearFrame(frameId)
}

// Size Return replacer's size, which tracks the number of evictable frames.
func (r *LRUKReplacer) Size() SizeT {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.currSize
}

// internal

func (r *LRUKReplacer) clearFrame(frameId FrameIdT) {
	delete(r.accessCount, frameId)
	delete(r.isEvictable, frameId)
	r.currSize--
}

func (r *LRUKReplacer) checkOverstep(frameId FrameIdT) {
	if frameId > FrameIdT(r.replacerSize) {
		panic(fmt.Errorf("[RecordAccess] %d upper than replacer size", frameId))
	}
}
