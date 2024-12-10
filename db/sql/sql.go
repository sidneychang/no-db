package sql

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/sidneychang/no-db/config"
	"github.com/sidneychang/no-db/db/engine"
)

type SQLDB struct {
	kv       *engine.DB
	tableLks map[string]*sync.Mutex // 存储每张表的锁
}

type Table struct {
	Name    string
	Columns []string
}

func NewSQLDB(options config.Options) (*SQLDB, error) {
	kv, err := engine.NewDB(options)
	if err != nil {
		return nil, err
	}
	return &SQLDB{
		kv:       kv,
		tableLks: make(map[string]*sync.Mutex), // Initialize the map here
	}, nil
}

// 获取指定表的锁
func (db *SQLDB) getTableLock(tableName string) *sync.Mutex {
	if db.tableLks == nil {
		db.tableLks = make(map[string]*sync.Mutex) // 确保 map 被初始化
	}
	lock, exists := db.tableLks[tableName]
	if !exists {
		lock = &sync.Mutex{}
		db.tableLks[tableName] = lock
	}
	return lock
}

// 创建表
func (db *SQLDB) CreateTable(tableName string, columns []string) error {
	lock := db.getTableLock(tableName)
	lock.Lock()
	defer lock.Unlock()

	table := Table{Name: tableName, Columns: columns}
	key := fmt.Sprintf("table:%s", tableName)
	value, _ := json.Marshal(table)
	return db.kv.Put([]byte(key), value)
}

// 删除表
func (db *SQLDB) DropTable(tableName string) error {
	lock := db.getTableLock(tableName)
	lock.Lock()
	defer lock.Unlock()

	key := fmt.Sprintf("table:%s", tableName)
	return db.kv.Delete([]byte(key))
}

// 插入数据
func (db *SQLDB) Insert(tableName string, data map[string]string) error {
	lock := db.getTableLock(tableName)
	lock.Lock()
	defer lock.Unlock()

	// 自动生成唯一 ID（如果未提供）
	if _, ok := data["id"]; !ok {
		data["id"] = uuid.New().String()
	}
	key := fmt.Sprintf("%s:%s", tableName, data["id"])
	value, _ := json.Marshal(data)
	return db.kv.Put([]byte(key), value)
}

// 查找数据，支持简单的 WHERE 语句// 扩展后的 Select 方法，支持多种条件和组合查询
func (db *SQLDB) Select(tableName string, whereClause map[string]string) ([]map[string]string, error) {
	keys := db.kv.GetListKeys() // 获取所有的键
	var results []map[string]string

	for _, key := range keys {
		if strings.HasPrefix(string(key), fmt.Sprintf("%s:", tableName)) {
			value, _ := db.kv.Get(key)
			var record map[string]string
			json.Unmarshal([]byte(value), &record)

			// 判断记录是否符合 WHERE 条件
			if matches(record, whereClause) {
				results = append(results, record)
			}
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i]["id"] < results[j]["id"]
	})
	return results, nil
}

// 辅助函数：支持多条件匹配的逻辑
// func matchesExtended(record map[string]string, whereClause map[string]interface{}) bool {
// 	for key, condition := range whereClause {
// 		switch v := condition.(type) {
// 		case string: // 简单等于
// 			if record[key] != v {
// 				return false
// 			}
// 		case map[string]interface{}: // 比较符操作
// 			for op, val := range v {
// 				switch op {
// 				case ">":
// 					if record[key] <= val.(string) {
// 						return false
// 					}
// 				case "<":
// 					if record[key] >= val.(string) {
// 						return false
// 					}
// 				case "=":
// 					if record[key] != val.(string) {
// 						return false
// 					}
// 				}
// 			}
// 		default:
// 			return false
// 		}
// 	}
// 	return true
// }

// 更新数据
// 异步更新操作
func (db *SQLDB) Update(tableName string, data map[string]string, whereClause map[string]string) error {
	lock := db.getTableLock(tableName)
	lock.Lock()
	defer lock.Unlock()

	records, err := db.Select(tableName, whereClause)
	if err != nil {
		return err
	}

	// 使用 Goroutine 并发更新
	var wg sync.WaitGroup
	for _, record := range records {
		wg.Add(1)
		go func(record map[string]string) {
			defer wg.Done()
			for k, v := range data {
				record[k] = v
			}
			key := fmt.Sprintf("%s:%s", tableName, record["id"])
			value, _ := json.Marshal(record)
			db.kv.Put([]byte(key), value)
		}(record)
	}
	wg.Wait()
	return nil
}

// 删除数据
func (db *SQLDB) Delete(tableName string, whereClause map[string]string) error {
	lock := db.getTableLock(tableName)
	lock.Lock()
	defer lock.Unlock()

	records, err := db.Select(tableName, whereClause)
	if err != nil {
		return err
	}
	for _, record := range records {
		key := fmt.Sprintf("%s:%s", tableName, record["id"])
		db.kv.Delete([]byte(key))
	}
	return nil
}

// 辅助函数：判断记录是否符合 WHERE 条件
func matches(record map[string]string, whereClause map[string]string) bool {
	for k, v := range whereClause {
		if record[k] != v {
			return false
		}
	}
	return true
}

// SelectWithPagination 支持分页
func (db *SQLDB) SelectWithPagination(tableName string, whereClause map[string]string, limit, offset int) ([]map[string]string, error) {
	allRecords, err := db.Select(tableName, whereClause)
	if err != nil {
		return nil, err
	}

	// 分页逻辑
	totalRecords := len(allRecords)
	if offset > totalRecords {
		return []map[string]string{}, nil
	}

	end := offset + limit
	if end > totalRecords {
		end = totalRecords
	}

	return allRecords[offset:end], nil
}
