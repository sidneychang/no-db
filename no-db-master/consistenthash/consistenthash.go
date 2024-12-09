package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// 虚拟节点数
const defaultVirtualNodes = 100

// HashRing 实现一致性哈希
type HashRing struct {
	virtualNodes int               // 每个物理节点的虚拟节点数
	nodes        []string          // 物理节点列表
	nodeHashes   map[uint32]string // 哈希值到物理节点的映射
	sortedHashes sortedHashes      // 排序后的哈希值
	mu           sync.RWMutex      // 锁，保护并发访问
}

// sortedHashes 是自定义的类型，包装了 uint32 类型的切片
type sortedHashes []uint32

// 实现 sort.Interface 接口方法
func (s sortedHashes) Len() int           { return len(s) }
func (s sortedHashes) Less(i, j int) bool { return s[i] < s[j] }
func (s sortedHashes) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// NewHashRing 创建一个新的带虚拟节点的一致性哈希环
func NewHashRing(nodes []string, virtualNodes int) *HashRing {
	if virtualNodes <= 0 {
		virtualNodes = defaultVirtualNodes
	}

	ring := &HashRing{
		virtualNodes: virtualNodes,
		nodes:        nodes,
		nodeHashes:   make(map[uint32]string),
	}

	// 将每个物理节点添加到哈希环
	for _, node := range nodes {
		ring.AddNode(node)
	}

	return ring
}

// AddNode 将物理节点及其虚拟节点加入哈希环
func (r *HashRing) AddNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 为每个物理节点创建多个虚拟节点
	for i := 0; i < r.virtualNodes; i++ {
		virtualNode := node + "#" + strconv.Itoa(i)
		hash := r.hash(virtualNode)

		// 记录虚拟节点的哈希值与物理节点的映射
		r.nodeHashes[hash] = node
		r.sortedHashes = append(r.sortedHashes, hash)
	}

	// 保持哈希值排序
	sort.Sort(r.sortedHashes)
}

// RemoveNode 从哈希环中移除节点及其虚拟节点
func (r *HashRing) RemoveNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// 删除节点的所有虚拟节点
	for i := 0; i < r.virtualNodes; i++ {
		virtualNode := node + "#" + strconv.Itoa(i)
		hash := r.hash(virtualNode)

		// 删除虚拟节点的哈希值
		delete(r.nodeHashes, hash)

		// 删除排序后的哈希值
		for i, v := range r.sortedHashes {
			if v == hash {
				r.sortedHashes = append(r.sortedHashes[:i], r.sortedHashes[i+1:]...)
				break
			}
		}
	}
}

// hash 使用 crc32 哈希函数来计算节点或键的哈希值
func (r *HashRing) hash(value string) uint32 {
	return crc32.ChecksumIEEE([]byte(value))
}

// Get 根据 key 查找对应的节点
func (r *HashRing) Get(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// 计算 key 的哈希值
	hash := r.hash(key)

	// 找到大于等于 hash 的最小的哈希值
	idx := sort.Search(len(r.sortedHashes), func(i int) bool {
		return r.sortedHashes[i] >= hash
	})

	// 如果没有找到，则环绕回到第一个节点
	if idx == len(r.sortedHashes) {
		idx = 0
	}

	// 返回该虚拟节点映射的物理节点
	return r.nodeHashes[r.sortedHashes[idx]]
}
