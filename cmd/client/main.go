package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sidneychang/no-db/consistanthash"
	pb "github.com/sidneychang/no-db/proto"
	"google.golang.org/grpc"
)

var connPool = make(map[string]*grpc.ClientConn) // 连接池
var hashRing *consistanthash.HashRing            // 一致性哈希环

func main() {
	// 创建一致性哈希环，包含所有节点的信息
	// hashRing = consistanthash.NewHashRing([]string{"192.168.1.2:50051", "192.168.1.3:50051"})
	hashRing = consistanthash.NewHashRing([]string{"0.0.0.0:50051", "0.0.0.0:50052"})
	// 创建扫描器读取用户输入
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("Welcome to the NO-DB CLI!")
	fmt.Println("Available commands: put <key> <value>, get <key>, delete <key>, exit")

	// 循环处理用户输入
	for {
		fmt.Print("Enter command: ")
		scanner.Scan()
		command := scanner.Text()
		command = strings.ToLower(command)

		parts := splitCommand(command)

		if len(parts) == 0 {
			continue
		}

		if parts[0] == "exit" {
			fmt.Println("Exiting the CLI.")
			break
		}

		switch parts[0] {
		case "put":
			if len(parts) != 3 {
				fmt.Println("Usage: put <key> <value>")
				continue
			}
			err := put(parts[1], parts[2])
			if err != nil {
				fmt.Printf("Error in put: %v\n", err)
			}

		case "get":
			if len(parts) != 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			err := get(parts[1])
			if err != nil {
				fmt.Printf("Error in get: %v\n", err)
			}

		case "delete":
			if len(parts) != 2 {
				fmt.Println("Usage: delete <key>")
				continue
			}
			err := delete(parts[1])
			if err != nil {
				fmt.Printf("Error in delete: %v\n", err)
			}

		default:
			fmt.Println("Unknown command:", parts[0])
		}
	}
}

func splitCommand(command string) []string {
	var parts []string
	for _, part := range strings.Fields(command) {
		parts = append(parts, part)
	}
	return parts
}
func put(key, value string) error {
	// 根据一致性哈希选择主节点和副本节点
	clientMain, err := getClientConnection(key)
	if err != nil {
		return err
	}

	replicaKey := key + "-rep"
	clientReplica, err := getClientConnection(replicaKey)
	if err != nil {
		return err
	}

	// 使用协程并行执行主键和副本的写入
	errChan := make(chan error, 2)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := clientMain.Put(ctx, &pb.PutRequest{Key: key, Value: value})
		errChan <- err
	}()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := clientReplica.Put(ctx, &pb.PutRequest{Key: replicaKey, Value: value})
		errChan <- err
	}()

	// 等待两个操作完成，检查错误
	var finalErr error
	for i := 0; i < 2; i++ {
		if err := <-errChan; err != nil {
			finalErr = err
		}
	}
	if finalErr == nil {
		fmt.Printf("Put: %s = %s (and replica)\n", key, value)
	} else {
		fmt.Printf("Put error: %v\n", finalErr)
	}
	return finalErr
}

func get(key string) error {
	// 根据一致性哈希选择主节点
	clientMain, err := getClientConnection(key)
	if err != nil {
		return err
	}

	replicaKey := key + "-rep"
	clientReplica, err := getClientConnection(replicaKey)
	if err != nil {
		return err
	}

	// 尝试获取主键的值
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := clientMain.Get(ctx, &pb.GetRequest{Key: key})
	if err == nil {
		fmt.Printf("Get: %s = %s\n", key, resp.Value)
		return nil
	}

	// 如果主键获取失败，尝试从副本获取
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err = clientReplica.Get(ctx, &pb.GetRequest{Key: replicaKey})
	if err == nil {
		fmt.Printf("Get (replica): %s = %s\n", key, resp.Value)
		return nil
	}

	return fmt.Errorf("Get failed: %v (and replica)", err)
}

func delete(key string) error {
	// 根据一致性哈希选择主节点和副本节点
	clientMain, err := getClientConnection(key)
	if err != nil {
		return err
	}

	replicaKey := key + "-rep"
	clientReplica, err := getClientConnection(replicaKey)
	if err != nil {
		return err
	}

	// 使用协程并行执行主键和副本的删除
	errChan := make(chan error, 2)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := clientMain.Delete(ctx, &pb.DeleteRequest{Key: key})
		errChan <- err
	}()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := clientReplica.Delete(ctx, &pb.DeleteRequest{Key: replicaKey})
		errChan <- err
	}()

	// 等待两个操作完成，检查错误
	var finalErr error
	for i := 0; i < 2; i++ {
		if err := <-errChan; err != nil {
			finalErr = err
		}
	}
	if finalErr == nil {
		fmt.Printf("Deleted: %s (and replica)\n", key)
	} else {
		fmt.Printf("Delete error: %v\n", finalErr)
	}
	return finalErr
}

// getClientConnection 使用一致性哈希来获取客户端连接
func getClientConnection(key string) (pb.KVDBClient, error) {
	// 根据一致性哈希找到目标节点
	nodeAddr := hashRing.Get(key)

	// 检查连接池中是否已存在该连接
	if conn, exists := connPool[nodeAddr]; exists {
		// 如果存在，直接返回现有的连接
		return pb.NewKVDBClient(conn), nil
	}

	// 如果不存在，建立新的连接并存储到连接池
	conn, err := grpc.Dial(nodeAddr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to server: %v", err)
	}

	connPool[nodeAddr] = conn // 将新的连接存入连接池
	return pb.NewKVDBClient(conn), nil
}
