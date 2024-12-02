package main

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"
)

func TestClient(t *testing.T) {
	client := NewClient([]string{"0.0.0.0:50051", "0.0.0.0:50052"}, 3)

	// 功能测试
	fmt.Println("Starting API functional tests...")
	// testPut(client, "key1", "value1")
	// testGet(client, "key1")
	// testDelete(client, "key1")

	// // 添加和删除节点
	// testAddNode(client, "0.0.0.0:50053")
	// testRemoveNode(client, "0.0.0.0:50053")

	// 性能测试
	fmt.Println("Starting performance tests...")
	performanceTest(client, 100000, 100)
}

func testPut(client *Client, key, value string) {
	fmt.Printf("Testing Put: %s = %s\n", key, value)
	if err := client.Put(key, value); err != nil {
		log.Fatalf("Put failed: %v", err)
	}
	fmt.Println("Put test passed.")
}

func testGet(client *Client, key string) {
	fmt.Printf("Testing Get: %s\n", key)
	if err := client.Get(key); err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	fmt.Println("Get test passed.")
}

func testDelete(client *Client, key string) {
	fmt.Printf("Testing Delete: %s\n", key)
	if err := client.Delete(key); err != nil {
		log.Fatalf("Delete failed: %v", err)
	}
	fmt.Println("Delete test passed.")
}

func testAddNode(client *Client, address string) {
	fmt.Printf("Testing AddNode: %s\n", address)
	client.AddNode(address)
	fmt.Println("AddNode test passed.")
}

func testRemoveNode(client *Client, address string) {
	fmt.Printf("Testing RemoveNode: %s\n", address)
	client.RemoveNode(address)
	fmt.Println("RemoveNode test passed.")
}

// 性能测试
func performanceTest(client *Client, requests int, concurrency int) {
	startTime := time.Now()
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()
			for j := 0; j < requests/concurrency; j++ {
				key := fmt.Sprintf("key-%d-%d", threadID, j)
				value := fmt.Sprintf("value-%d-%d", threadID, j)
				if err := client.Put(key, value); err != nil {
					log.Printf("Put failed: %v", err)
				}
				if err := client.Get(key); err != nil {
					log.Printf("Get failed: %v", err)
				}
				if err := client.Delete(key); err != nil {
					log.Printf("Delete failed: %v", err)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)
	fmt.Printf("Performance test completed in %v for %d requests.\n", duration, requests)
}
