package index

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	"github.com/chen3feng/stl4go"
	"github.com/sidneychang/no-db/db/data"
)

// SkipList Memory Index
type SkipList struct {
	list *stl4go.SkipList[[]byte, *data.RecordPst]
	lock *sync.RWMutex
}

func NewSkipList() *SkipList {
	return &SkipList{
		list: stl4go.NewSkipListFunc[[]byte, *data.RecordPst](Compare),
		lock: new(sync.RWMutex),
	}
}

func (s *SkipList) Put(key []byte, pst *data.RecordPst) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.list.Insert(key, pst)
	return true
}

func (s *SkipList) Get(key []byte) *data.RecordPst {
	s.lock.RLock()
	defer s.lock.RUnlock()
	fmt.Println("searching key")
	res := s.list.Find(key)
	if res != nil {
		return *res
	}
	return nil

}

func (s *SkipList) Size() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.list.Len()
}
func (s *SkipList) Delete(key []byte) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	return s.list.Remove(key)
}

func (s *SkipList) Iterator(reverse bool) Iterator {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return NewSkipListIterator(s, reverse)
}

type SkipListIterator struct {
	currentIndex int
	reverse      bool
	values       []*Item
}

func NewSkipListIterator(s *SkipList, reverse bool) *SkipListIterator {
	expectedSize := s.Size()

	values := make([]*Item, 0, expectedSize)

	saveToValues := func(K []byte, V *data.RecordPst) {
		item := &Item{
			key: K,
			pst: V,
		}
		values = append(values, item)
	}
	s.list.ForEach(saveToValues)
	if reverse {
		for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
			values[i], values[j] = values[j], values[i]
		}
	}
	return &SkipListIterator{
		currentIndex: 0,
		values:       values,
		reverse:      reverse,
	}

}
func (si *SkipListIterator) Rewind() {
	si.currentIndex = 0
}

func (si *SkipListIterator) Seek(key []byte) {
	if si.reverse {
		si.currentIndex = sort.Search(len(si.values), func(i int) bool {
			return bytes.Compare(si.values[i].key, key) <= 0
		})
	} else {
		si.currentIndex = sort.Search(len(si.values), func(i int) bool {
			return bytes.Compare(si.values[i].key, key) >= 0
		})
	}
}

func (si *SkipListIterator) Next() {

	si.currentIndex++

}

func (si *SkipListIterator) Valid() bool {
	return si.currentIndex < len(si.values)
}

func (si *SkipListIterator) Key() []byte {
	return si.values[si.currentIndex].key
}
func (si *SkipListIterator) Value() *data.RecordPst {
	return si.values[si.currentIndex].pst
}

func (si *SkipListIterator) Close() {
	si.values = nil
}
