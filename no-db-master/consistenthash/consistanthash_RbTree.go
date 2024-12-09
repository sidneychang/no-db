package consistenthash

import (
	"context"
	"fmt"
	"hash/crc32"
	"strconv"
	"sync"
	"time"

	rb "github.com/sidneychang/no-db/RbTree"
	pb "github.com/sidneychang/no-db/proto"
	"google.golang.org/grpc"
)

// 虚拟节点数
const DefaultVirtualNodes = 100

type RbHashRing struct {
	virtualNodes int      // 每个物理节点的虚拟节点数
	nodes        []string // 所有节点的列表
	rbTree       rb.RbTree
	mu           sync.RWMutex // 锁，保护并发访问
}

func NewRbHashRing(nodes []string, virtualNodes int) *RbHashRing {
	if virtualNodes <= 0 {
		virtualNodes = DefaultVirtualNodes
	}

	ring := &RbHashRing{
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
func (r *RbHashRing) AddNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	NewConn, err := grpc.Dial(node, grpc.WithInsecure())
	if err != nil {
		fmt.Errorf("Failed To Connect To Server: %v", err)
	}
	defer NewConn.Close()
	NewClient := pb.NewKVDBClient(NewConn)
	if err != nil {
		fmt.Errorf("Connected failed: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var Keys []string
	var Values []string
	for i := 0; i < r.virtualNodes; i++ {
		virtualNode := node + "#" + strconv.Itoa(i)
		hash := r.hash(virtualNode)
		NextNode := r.rbTree.FindNextNode(rb.RbTreeKeyType(hash))
		PreNode := r.rbTree.FindPreNode(rb.RbTreeKeyType(hash))

		if NextNode == r.rbTree.Sentinel {
			continue
		}
		NextNodeValue, ok := NextNode.Value.(string)
		if !ok {
			fmt.Errorf("expected a string, got %T", NextNodeValue)
			return
		}
		PreNodeKey := PreNode.Key
		// if !ok {
		// 	fmt.Errorf("expected a string, got %T", PreNodeKey)
		// 	return
		// }
		NextConn, err := grpc.Dial(NextNodeValue, grpc.WithInsecure())
		if err != nil {
			fmt.Errorf("Failed To Connect To Server: %v", err)
			return
		}
		defer NextConn.Close()
		NextClinent := pb.NewKVDBClient(NextConn)
		if err != nil {
			fmt.Errorf("Connected failed: %v", err)
		}
		resp, err := NextClinent.ListAllData(ctx, &pb.Empty{})

		if err != nil {
			fmt.Errorf("GetAllData failed: %v", err)
			return
		}
		for j := 0; j < len(resp.Keys); j++ {
			NowHash := r.hash(resp.Keys[j])
			if Judge(uint32(PreNodeKey), hash, NowHash) {
				NextClinent.Delete(ctx, &pb.DeleteRequest{Key: resp.Keys[j]})
				Keys = append(Keys, resp.Keys[j])
				Values = append(Values, resp.Values[j])
			}
		}
	}

	for i := 0; i < r.virtualNodes; i++ {
		virtualNode := node + "#" + strconv.Itoa(i)
		hash := r.hash(virtualNode)
		newNode := r.rbTree.NewRbTreeNode(rb.RbTreeKeyType(hash), node)
		r.rbTree.InsertNewNode(newNode)
	}

	for i := 0; i < len(Keys); i++ {
		NewClient.Put(ctx, &pb.PutRequest{Key: Keys[i], Value: Values[i]})
		fmt.Printf("AddNode to Put %s - %s\n", Keys[i], Values[i])
	}
}

func Judge(pre uint32, next uint32, now uint32) bool {
	if next >= pre {
		return now > pre && next >= now
	}
	return now > pre || next >= now
}

// removeNode 从哈希环中移除节点
func (r *RbHashRing) RemoveNode(node string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := 0; i < r.virtualNodes; i++ {
		virtualNode := node + "#" + strconv.Itoa(i)
		hash := r.hash(virtualNode)
		r.rbTree.DeleteByKey(rb.RbTreeKeyType(hash))
	}
}

func (r *RbHashRing) hash(value string) uint32 {
	return crc32.ChecksumIEEE([]byte(value))
}

// Get 根据 key 查找对应的节点
func (r *RbHashRing) Get(key string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// 计算 key 的哈希值
	hash := r.hash(key)
	// fmt.Printf("%s %d\n", key, hash)
	node := r.rbTree.FindMaxKey(rb.RbTreeKeyType(hash))
	value, ok := node.Value.(string)
	if !ok {
		fmt.Errorf("expected a string, got %T", value)
	}
	// fmt.Printf("%s %d\n", value, node.Key)
	return value
}

// func (r *RbHashRing) NewAddNode(key uint32) {
// 	NextNode := r.rbTree.FindNextNode(rb.Uint32Key{Value: key})

// }
