package engine

import (
	"bytes"

	"github.com/sidneychang/no-db/config"
	"github.com/sidneychang/no-db/db/index"
)

type Iterator struct {
	indexIter index.Iterator
	db        *DB
	options   config.IteratorOptions
}

func (db *DB) NewIterator(options config.IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(options.Reverse)
	return &Iterator{
		indexIter: indexIter,
		db:        db,
		options:   options,
	}
}
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.skipToNext()
}
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.skipToNext()
}
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.skipToNext()
}
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}
func (it *Iterator) Value() ([]byte, error) {
	RecordPst := it.indexIter.Value()
	it.db.lock.RLock()
	defer it.db.lock.RUnlock()

	return it.db.getValueByPosition(RecordPst)
}
func (it *Iterator) Close() {
	it.indexIter.Close()
}
func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}
	for ; it.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixLen <= len(key) && bytes.Equal(it.options.Prefix, key[:prefixLen]) {
			break

		}

	}
}
