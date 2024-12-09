package index

import (
	"bytes"

	"github.com/sidneychang/no-db/db/data"
)

type Indexer interface {
	Put(key []byte, pst *data.RecordPst) bool
	Get(key []byte) *data.RecordPst
	Delete(key []byte) bool
	Size() int
	Iterator(reverse bool) Iterator
}

func NewIndexer(dirPath string) Indexer {
	return NewSkipList()
}

type Item struct {
	key []byte
	pst *data.RecordPst
}

// Iterator is a generic index iterator.
type Iterator interface {
	// Rewind resets the iterator to the beginning, i.e., the first entry.
	Rewind()

	// Seek seeks to a target key that is >= or <= the given key, depending on the implementation.
	Seek(key []byte)

	// Next moves to the next key.
	Next()

	// Valid returns whether the iterator is still valid, i.e., if all keys have been traversed.
	Valid() bool

	// Key returns the key at the current iterator position.
	Key() []byte

	// Value returns the value (position information) at the current iterator position.
	Value() *data.RecordPst

	// Close closes the iterator and releases any resources.
	Close()
}

func Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}
