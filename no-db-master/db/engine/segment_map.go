package engine;


import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

const numSegments = 16 // 分段的数量

// SegmentedMap 是一个包含多个段的线程安全的 Map
type SegmentedMap struct {
    segments [numSegments]map[string]interface{}
    locks    [numSegments]sync.RWMutex
}

func NewSegmentedMap() *SegmentedMap {
    sm := &SegmentedMap{}
    for i := 0; i < numSegments; i++ {
        sm.segments[i] = make(map[string]interface{})
    }
    return sm
}

// getSegmentIndex 获取元素的段索引，通常是哈希值的一个简单操作
func (sm *SegmentedMap) getSegmentIndex(key string) int {
    return int(hashKey(key)) % numSegments
}

// hashKey 哈希函数，用于确定 key 所在的段
func hashKey(key string) uint32 {
    var hash uint32
    for i := 0; i < len(key); i++ {
        hash = hash*31 + uint32(key[i])
    }
    return hash
}

// Set 设置 key 的值
func (sm *SegmentedMap) Set(key string, value interface{}) {
    segmentIndex := sm.getSegmentIndex(key)
    sm.locks[segmentIndex].Lock() // 获取锁
    defer sm.locks[segmentIndex].Unlock()

    sm.segments[segmentIndex][key] = value
}

// Get 获取 key 对应的值
func (sm *SegmentedMap) Get(key string) (interface{}, bool) {
    segmentIndex := sm.getSegmentIndex(key)
    sm.locks[segmentIndex].RLock() // 获取读锁
    defer sm.locks[segmentIndex].RUnlock()

    value, exists := sm.segments[segmentIndex][key]
    return value, exists
}

// Delete 删除 key
func (sm *SegmentedMap) Delete(key string) {
    segmentIndex := sm.getSegmentIndex(key)
    sm.locks[segmentIndex].Lock() // 获取锁
    defer sm.locks[segmentIndex].Unlock()

    delete(sm.segments[segmentIndex], key)
}
// LoadData 从磁盘加载数据，path 用于指定存储目录
func (sm *SegmentedMap) LoadData(path string) error {
	for i := 0; i < numSegments; i++ {
		filePath := fmt.Sprintf("%s/segment%d.json", path, i)
		file, err := os.Open(filePath)
		if err != nil {
			// 如果文件不存在，跳过该段，初始化为空 map
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("failed to open file: %w", err)
		}
		defer file.Close()

		decoder := json.NewDecoder(file)
		err = decoder.Decode(&sm.segments[i])
		if err != nil {
			return fmt.Errorf("failed to decode data: %w", err)
		}
	}
	return nil
}

// SaveData 将数据保存到磁盘，path 用于指定存储目录
func (sm *SegmentedMap) SaveData(path string) error {
	for i := 0; i < numSegments; i++ {
		filePath := fmt.Sprintf("%s/segment%d.json", path, i)
		file, err := os.Create(filePath)
		if err != nil {
			return fmt.Errorf("failed to create file: %w", err)
		}
		defer file.Close()

		encoder := json.NewEncoder(file)
		err = encoder.Encode(sm.segments[i])
		if err != nil {
			return fmt.Errorf("failed to encode data: %w", err)
		}
	}
	return nil
}
func (sm *SegmentedMap) GetAll() map[string]interface{} {
	allData := make(map[string]interface{})

	// 遍历每个段
	for i := 0; i < numSegments; i++ {
		sm.locks[i].RLock() // 获取读锁
		for key, value := range sm.segments[i] {
			allData[key] = value // 将每个段中的键值对添加到 allData
		}
		sm.locks[i].RUnlock() // 释放读锁
	}

	return allData
}
