package consistenthash

// HashRingInterface 定义了哈希环的公共接口
type HashRingInterface interface {
	AddNode(node string)
	RemoveNode(node string)
	Get(key string) string
}
