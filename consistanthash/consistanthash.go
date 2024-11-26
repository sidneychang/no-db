package consistanthash

import (
	"hash/crc32"
	"sort"
	"sync"
)

// HashRing 实现一致性哈希
type HashRing struct {
	nodes        []string          // 所有节点的列表
	nodeHashes   map[uint32]string // 哈希到节点的映射
	sortedHashes sortedHashes      // 排序后的哈希值
	mu           sync.RWMutex      // 锁，保护并发访问
}

// sortedHashes 是自定义的类型，包装了 uint32 类型的切片
type sortedHashes []uint32

// 实现 sort.Interface 接口方法

func (s sortedHashes) Len() int {
	return len(s)
}

func (s sortedHashes) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s sortedHashes) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// NewHashRing 创建一个新的一致性哈希环
func NewHashRing(nodes []string) *HashRing {
	ring := &HashRing{
		nodes:      nodes,
		nodeHashes: make(map[uint32]string),
	}

	// 将每个节点添加到哈希环
	for _, node := range nodes {
		ring.addNode(node)
	}

	return ring
}

// addNode 将节点加入哈希环
func (r *HashRing) addNode(node string) {
	hash := r.hash(node)
	r.mu.Lock()
	defer r.mu.Unlock()

	// 将节点的哈希值与节点名称关联
	r.nodeHashes[hash] = node
	r.sortedHashes = append(r.sortedHashes, hash)

	// 保持哈希值排序
	sort.Sort(r.sortedHashes)
}

// removeNode 从哈希环中移除节点
func (r *HashRing) removeNode(node string) {
	hash := r.hash(node)
	r.mu.Lock()
	defer r.mu.Unlock()

	// 删除哈希值
	delete(r.nodeHashes, hash)

	// 删除排序后的哈希值
	for i, v := range r.sortedHashes {
		if v == hash {
			r.sortedHashes = append(r.sortedHashes[:i], r.sortedHashes[i+1:]...)
			break
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

	// 返回该节点的地址
	node := r.nodeHashes[r.sortedHashes[idx]]
	return node
}
