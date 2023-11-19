package lruk

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/pjimming/HermesDB/define"
)

func TestSampleLRUK(t *testing.T) {
	ast := assert.New(t)

	lruReplacer := NewLRUKReplacer(7, 2)

	// Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is non-evictable.
	lruReplacer.RecordAccess(1)
	lruReplacer.RecordAccess(2)
	lruReplacer.RecordAccess(3)
	lruReplacer.RecordAccess(4)
	lruReplacer.RecordAccess(5)
	lruReplacer.RecordAccess(6)
	lruReplacer.SetEvictable(1, true)
	lruReplacer.SetEvictable(2, true)
	lruReplacer.SetEvictable(3, true)
	lruReplacer.SetEvictable(4, true)
	lruReplacer.SetEvictable(5, true)
	lruReplacer.SetEvictable(6, false)

	ast.Equal(define.SizeT(5), lruReplacer.Size())

	// Scenario: Insert access history for frame 1. Now frame 1 has two access histories.
	// All other frames have max backward k-dist. The order of eviction is [2,3,4,5,1].
	lruReplacer.RecordAccess(1)

	// Scenario: Evict three pages from the replacer. Elements with max k-distance should be popped
	// first based on LRU.
	var value define.FrameIdT
	var isEvicted bool
	value, _ = lruReplacer.Evict()
	ast.Equal(define.FrameIdT(2), value)
	value, _ = lruReplacer.Evict()
	ast.Equal(define.FrameIdT(3), value)
	value, _ = lruReplacer.Evict()
	ast.Equal(define.FrameIdT(4), value)
	ast.Equal(define.SizeT(2), lruReplacer.Size())

	// Scenario: Now replacer has frames [5,1].
	// Insert new frames 3, 4, and update access history for 5. We should end with [3,1,5,4]
	lruReplacer.RecordAccess(3)
	lruReplacer.RecordAccess(4)
	lruReplacer.RecordAccess(5)
	lruReplacer.RecordAccess(4)
	lruReplacer.SetEvictable(3, true)
	lruReplacer.SetEvictable(4, true)
	ast.Equal(define.SizeT(4), lruReplacer.Size())

	// Scenario: continue looking for victims. We expect 3 to be evicted next.
	value, _ = lruReplacer.Evict()
	ast.Equal(define.FrameIdT(3), value)
	ast.Equal(define.SizeT(3), lruReplacer.Size())

	// Set 6 to be evictable. 6 Should be evicted next since it has max backward k-dist.
	lruReplacer.SetEvictable(6, true)
	ast.Equal(define.SizeT(4), lruReplacer.Size())
	value, _ = lruReplacer.Evict()
	ast.Equal(define.FrameIdT(6), value)
	ast.Equal(define.SizeT(3), lruReplacer.Size())

	// Now we have [1,5,4]. Continue looking for victims.
	lruReplacer.SetEvictable(1, false)
	ast.Equal(define.SizeT(2), lruReplacer.Size())
	value, isEvicted = lruReplacer.Evict()
	ast.Equal(true, isEvicted)
	ast.Equal(define.FrameIdT(5), value)
	ast.Equal(define.SizeT(1), lruReplacer.Size())

	// Update access history for 1. Now we have [4,1]. Next victim is 4.
	lruReplacer.RecordAccess(1)
	lruReplacer.RecordAccess(1)
	lruReplacer.SetEvictable(1, true)
	ast.Equal(define.SizeT(2), lruReplacer.Size())
	value, isEvicted = lruReplacer.Evict()
	ast.Equal(true, isEvicted)
	ast.Equal(define.FrameIdT(4), value)

	ast.Equal(define.SizeT(1), lruReplacer.Size())
	value, isEvicted = lruReplacer.Evict()
	ast.Equal(true, isEvicted)
	ast.Equal(define.FrameIdT(1), value)
	ast.Equal(define.SizeT(0), lruReplacer.Size())

	// These operations should not modify size
	_, isEvicted = lruReplacer.Evict()
	ast.Equal(false, isEvicted)
	ast.Equal(define.SizeT(0), lruReplacer.Size())
	lruReplacer.Remove(1)
	ast.Equal(define.SizeT(0), lruReplacer.Size())
}
