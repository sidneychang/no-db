package engine

import (
	"fmt"
	"os"
	"sync"
	"testing"
)


const testPath = "./testdata"

// 在每个测试运行之前，确保清空 testdata 目录
func clearTestData() {
	err := os.RemoveAll(testPath)
	if err != nil {
		fmt.Printf("Error cleaning test data directory: %v\n", err)
	}
	err = os.Mkdir(testPath, os.ModePerm)
	if err != nil {
		fmt.Printf("Error creating test data directory: %v\n", err)
	}
}

// TestSegmentedMap_SetAndGet 测试 Set 和 Get
func TestSegmentedMap_SetAndGet(t *testing.T) {
	sm := NewSegmentedMap()

	sm.Set("key1", "value1")
	sm.Set("key2", "value2")
	sm.Set("key3", "value3")

	// 测试获取已设置的值
	if value, exists := sm.Get("key1"); !exists || value != "value1" {
		t.Errorf("Expected 'value1' for key 'key1', got '%v'", value)
	}

	if value, exists := sm.Get("key2"); !exists || value != "value2" {
		t.Errorf("Expected 'value2' for key 'key2', got '%v'", value)
	}

	if value, exists := sm.Get("key3"); !exists || value != "value3" {
		t.Errorf("Expected 'value3' for key 'key3', got '%v'", value)
	}
}

// TestSegmentedMap_Delete 测试 Delete
func TestSegmentedMap_Delete(t *testing.T) {
	sm := NewSegmentedMap()

	sm.Set("key1", "value1")
	sm.Delete("key1")

	// 测试删除后的值
	if _, exists := sm.Get("key1"); exists {
		t.Error("Expected key 'key1' to be deleted, but it still exists")
	}
}

// TestSegmentedMap_Concurrency 测试并发
func TestSegmentedMap_Concurrency(t *testing.T) {
	sm := NewSegmentedMap()

	// 并发测试：多个 goroutine 设置不同的 key
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", i)
			sm.Set(key, i)
		}(i)
	}

	wg.Wait()

	// 测试并发设置的值
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		if value, exists := sm.Get(key); !exists || value != i {
			t.Errorf("Expected value %d for %s, got %v", i, key, value)
		}
	}
}

// TestSegmentedMap_LoadAndSave 测试 Load 和 Save
func TestSegmentedMap_LoadAndSave(t *testing.T) {
	clearTestData() // 清理测试目录

	sm := NewSegmentedMap()

	// 设置一些数据
	sm.Set("key1", "value1")
	sm.Set("key2", "value2")
	sm.Set("key3", "value3")

	// 保存数据到磁盘
	err := sm.SaveData(testPath)
	if err != nil {
		t.Fatalf("Failed to save data: %v", err)
	}

	// 创建一个新的 SegmentedMap 实例并加载数据
	newSm := NewSegmentedMap()
	err = newSm.LoadData(testPath)
	if err != nil {
		t.Fatalf("Failed to load data: %v", err)
	}

	// 验证加载的数据
	if value, exists := newSm.Get("key1"); !exists || value != "value1" {
		t.Errorf("Expected 'value1' for key 'key1', got '%v'", value)
	}

	if value, exists := newSm.Get("key2"); !exists || value != "value2" {
		t.Errorf("Expected 'value2' for key 'key2', got '%v'", value)
	}

	if value, exists := newSm.Get("key3"); !exists || value != "value3" {
		t.Errorf("Expected 'value3' for key 'key3', got '%v'", value)
	}
}

// TestSegmentedMap_LoadWithMissingData 测试加载缺失的数据
func TestSegmentedMap_LoadWithMissingData(t *testing.T) {
	clearTestData() // 清理测试目录

	sm := NewSegmentedMap()

	// 保存数据到磁盘（但不包含任何数据）
	err := sm.SaveData(testPath)
	if err != nil {
		t.Fatalf("Failed to save empty data: %v", err)
	}

	// 加载数据
	newSm := NewSegmentedMap()
	err = newSm.LoadData(testPath)
	if err != nil {
		t.Fatalf("Failed to load data: %v", err)
	}

	// 验证加载的数据应该为空
	if len(newSm.segments) != numSegments {
		t.Fatalf("Expected %d segments, got %d", numSegments, len(newSm.segments))
	}
}
