package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sidneychang/no-db/consistenthash"
	pb "github.com/sidneychang/no-db/proto"
	"google.golang.org/grpc"
)

// Client 结构体，用于封装一致性哈希和 gRPC 连接池
type Client struct {
	hashRing consistenthash.HashRingInterface // 一致性哈希环
	connPool map[string]*grpc.ClientConn      // 连接池
}

// NewClient 创建一个新的 Client 实例
func NewClient(nodes []string, replicas int) *Client {
	var useRb = true
	var hashRing consistenthash.HashRingInterface
	if useRb {
		hashRing = consistenthash.NewRbHashRing(nodes, replicas)
	} else {
		hashRing = consistenthash.NewHashRing(nodes, replicas)
	}
	return &Client{
		hashRing: hashRing,
		connPool: make(map[string]*grpc.ClientConn),
	}
}

func main() {
	client := NewClient([]string{"0.0.0.0:50051"}, 3)

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Welcome to the NO-DB CLI!")
	fmt.Println("Available commands: put <key> <value>, get <key>, delete <key>, addnode <address>, deletenode <address>, exit")

	for {
		fmt.Print("Enter command: ")
		scanner.Scan()
		command := scanner.Text()
		command = strings.ToLower(command)

		parts := splitCommand(command)
		if len(parts) == 0 {
			continue
		}

		switch parts[0] {
		case "exit":
			fmt.Println("Exiting the CLI.")
			return
		case "put":
			if len(parts) != 3 {
				fmt.Println("Usage: put <key> <value>")
				continue
			}
			if err := client.Put(parts[1], parts[2]); err != nil {
				fmt.Printf("Error in put: %v\n", err)
			}
		case "get":
			if len(parts) != 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			if err := client.Get(parts[1]); err != nil {
				fmt.Printf("Error in get: %v\n", err)
			}
		case "delete":
			if len(parts) != 2 {
				fmt.Println("Usage: delete <key>")
				continue
			}
			if err := client.Delete(parts[1]); err != nil {
				fmt.Printf("Error in delete: %v\n", err)
			}
		case "addnode":
			if len(parts) != 2 {
				fmt.Println("Usage: addnode <address>")
				continue
			}
			client.AddNode(parts[1])
		case "deletenode":
			if len(parts) != 2 {
				fmt.Println("Usage: addnode <address>")
				continue
			}
			client.DeleteNode(parts[1])
		case "removenode":
			if len(parts) != 2 {
				fmt.Println("Usage: removenode <address>")
				continue
			}
			client.RemoveNode(parts[1])
		default:
			fmt.Println("Unknown command:", parts[0])
		}
	}
}

func splitCommand(command string) []string {
	return strings.Fields(command)
}

// Put 将键值对存储到主节点和副本节点
func (c *Client) Put(key, value string) error {
	clientMain, err := c.getClientConnection(key)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = clientMain.Put(ctx, &pb.PutRequest{Key: key, Value: value})
	return err

}

// Get 获取指定键的值
func (c *Client) Get(key string) error {
	clientMain, err := c.getClientConnection(key)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := clientMain.Get(ctx, &pb.GetRequest{Key: key})
	if err == nil {
		fmt.Printf("Get: %s = %s\n", key, resp.Value)
		return nil
	}

	return fmt.Errorf("Get failed: %v (and replica)", err)
}

// Delete 删除指定键的值
func (c *Client) Delete(key string) error {
	clientMain, err := c.getClientConnection(key)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = clientMain.Delete(ctx, &pb.DeleteRequest{Key: key})

	return err
}

// AddNode 添加新节点到哈希环
func (c *Client) AddNode(address string) {
	c.hashRing.AddNode(address)
	fmt.Printf("Node %s added to the hash ring.\n", address)
}

// RemoveNode 从哈希环中删除节点
func (c *Client) RemoveNode(address string) {
	c.hashRing.RemoveNode(address)
	c.removeClientConnection(address)
	fmt.Printf("Node %s removed from the hash ring.\n", address)
}

// getClientConnection 获取指定键的 gRPC 连接
func (c *Client) getClientConnection(key string) (pb.KVDBClient, error) {
	nodeAddr := c.hashRing.Get(key)
	// fmt.Println(nodeAddr)
	if conn, exists := c.connPool[nodeAddr]; exists {
		return pb.NewKVDBClient(conn), nil
	}
	conn, err := grpc.Dial(nodeAddr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("Failed To Connect To Server: %v", err)
	}
	c.connPool[nodeAddr] = conn
	return pb.NewKVDBClient(conn), nil
}

func (c *Client) getClientConnectionByNode(nodeAddr string) (pb.KVDBClient, error) {
	if conn, exists := c.connPool[nodeAddr]; exists {
		return pb.NewKVDBClient(conn), nil
	}
	conn, err := grpc.Dial(nodeAddr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("Failed To Connect To Server: %v", err)
	}
	c.connPool[nodeAddr] = conn
	return pb.NewKVDBClient(conn), nil
}

// removeClientConnection 从连接池中删除节点连接
func (c *Client) removeClientConnection(address string) {
	if conn, exists := c.connPool[address]; exists {
		conn.Close()
		delete(c.connPool, address)
	}
}

func (c *Client) ListAllData(node string) ([]string, []string) {
	clientMain, err := c.getClientConnectionByNode(node)
	if err != nil {
		fmt.Errorf("Connected failed: %v", err)
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	resp, err := clientMain.ListAllData(ctx, &pb.Empty{})
	if err != nil {
		fmt.Errorf("GetAllData failed: %v", err)
		return nil, nil
	}

	return resp.Keys, resp.Values
}

func (c *Client) DeleteNode(node string) {
	Keys, Values := c.ListAllData(node)
	c.hashRing.RemoveNode(node)

	clientMain, err := c.getClientConnectionByNode(node)
	if err != nil {
		fmt.Errorf("Connected failed: %v", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for i := 0; i < len(Keys); i++ {
		clientMain.Delete(ctx, &pb.DeleteRequest{Key: Keys[i]})
	}

	for i := 0; i < len(Keys); i++ {
		c.Put(Keys[i], Values[i])
	}
	fmt.Printf("Node %s removed from the hash ring.\n", node)
}
