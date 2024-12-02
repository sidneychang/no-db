package index

import (
	"fmt"
	"sync"

	rb "github.com/sidneychang/no-db/RbTree"
	"github.com/sidneychang/no-db/db/data"
)

type RbTreeIndex struct {
	Index rb.RbTree
	lock  *sync.RWMutex
}

func NewRbTreeIndex() *RbTreeIndex {
	return &RbTreeIndex{
		Index: *rb.NewRbTree(),
		lock:  new(sync.RWMutex),
	}
}

func (s *RbTreeIndex) Put(key []byte, pst *data.RecordPst) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	newNode := s.Index.NewRbTreeNode(rb.BytesKey{Value: key}, pst)
	s.Index.InsertNewNode(newNode)
	return true
}

func (s *RbTreeIndex) Get(key []byte) *data.RecordPst {
	s.lock.RLock()
	defer s.lock.RUnlock()
	fmt.Println("searching key")
	node := s.Index.FindMaxKey(rb.BytesKey{Value: key})
	if node == s.Index.Sentinel {
		return nil
	}
	res, ok := node.Value.(data.RecordPst)
	if !ok {
		fmt.Errorf("expected a string, got %T", res)
	}
	return &res
}

func (s *RbTreeIndex) Size() int {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.Index.NodeNum
}

func (s *RbTreeIndex) Delete(key []byte) bool {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.Index.DeleteByKey(rb.BytesKey{Value: key})

	return true
}

type RbTreeIndexIterator struct {
	currentIndex rb.RbTreeNode
	values       rb.RbTree
}
