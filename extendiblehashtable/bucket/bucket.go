package bucket

import (
	"container/list"

	. "github.com/pjimming/HermesDB/define"
)

type Bucket struct {
	size  SizeT
	depth SizeT
	list  *list.List
}

type Entry struct {
	key   string
	value any
}

func NewBucket(size, depth SizeT) *Bucket {
	return &Bucket{
		size:  size,
		depth: depth,
		list:  list.New(),
	}
}

func (b *Bucket) Find(key string) (any, bool) {
	for e := b.list.Front(); e != nil; e = e.Next() {
		entry := e.Value.(*Entry)

		if entry.key == key {
			return entry.value, true
		}
	}
	return nil, false
}

func (b *Bucket) Remove(key string) bool {
	for e := b.list.Front(); e != nil; e = e.Next() {
		entry := e.Value.(*Entry)

		if entry.key == key {
			b.list.Remove(e)
			return true
		}
	}
	return false
}

func (b *Bucket) Insert(key string, value any) bool {
	for e := b.list.Front(); e != nil; e = e.Next() {
		entry := e.Value.(*Entry)
		if entry.key == key {
			entry.value = value
			return true
		}
	}

	if b.IsFull() {
		return false
	}
	b.list.PushBack(&Entry{key: key, value: value})
	return true
}

func (b *Bucket) IsFull() bool { return b.size == SizeT(b.list.Len()) }

func (b *Bucket) GetDepth() SizeT { return b.depth }

func (b *Bucket) IncrementDepth() { b.depth++ }

func (b *Bucket) GetItems() *list.List { return b.list }

// == Entry ==

func (e *Entry) GetKey() string {
	return e.key
}

func (e *Entry) GetValue() any {
	return e.value
}
