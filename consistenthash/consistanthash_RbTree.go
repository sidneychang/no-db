package consistenthash

import (
	"fmt"
	"hash/crc32"
	"strconv"
	"sync"

	rb "github.com/sidneychang/no-db/RbTree"
)

// 虚拟节点数
const DefaultVirtualNodes = 100

type HashRing struct {
	virtualNodes int      // 每个物理节点的虚拟节点数
	nodes        []string // 所有节点的列表
	rbTree       rb.RbTree
	mu           sync.RWMutex // 锁，保护并发访问
}

func NewHashRing(nodes []string, virtualNodes int) *HashRing {
	if virtualNodes <= 0 {
		virtualNodes = DefaultVirtualNodes
	}

	ring := &HashRing{
		virtualNodes: virtualNodes,
		nodes:        nodes,
		rbTree:       *rb.NewRbTree(),
	}

	// 将每个节点添加到哈希环
	for _, node := range nodes {
		ring.AddNode(node)
	}

	return ring
}

// addNode 将节点加入哈希环
func (r *HashRing) AddNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.virtualNodes; i++ {
		virtualNode := node + "#" + strconv.Itoa(i)
		hash := r.hash(virtualNode)
		newNode := r.rbTree.NewRbTreeNode(rb.Uint32Key{Value: hash}, node)
		r.rbTree.InsertNewNode(newNode)
	}
}

// removeNode 从哈希环中移除节点
func (r *HashRing) RemoveNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i := 0; i < r.virtualNodes; i++ {
		virtualNode := node + "#" + strconv.Itoa(i)
		hash := r.hash(virtualNode)
		r.rbTree.DeleteByKey(rb.Uint32Key{Value: hash})
	}

}

func (r *HashRing) hash(value string) uint32 {
	return crc32.ChecksumIEEE([]byte(value))
}

// Get 根据 key 查找对应的节点
func (r *HashRing) Get(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// 计算 key 的哈希值
	hash := r.hash(key)

	node := r.rbTree.FindMaxKey(rb.Uint32Key{Value: hash})
	if node == r.rbTree.Sentinel {
		node = r.rbTree.Root
	}
	value, ok := node.Value.(string)
	if !ok {
		fmt.Errorf("expected a string, got %T", value)
	}
	return value
}
