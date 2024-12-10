package sql

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/sidneychang/no-db/config"
	"github.com/sidneychang/no-db/db/engine"
	"github.com/stretchr/testify/assert"
)

func setup() *SQLDB {
	// 假设创建一个内存数据库配置
	options := config.NewOptions(1, 1024, os.TempDir())

	kv, err := engine.NewDB(*options)
	if err != nil {
		panic("Failed to create KV database")
	}
	return &SQLDB{kv: kv}
}

func TestCreateTable(t *testing.T) {
	db := setup()

	// 创建一个表
	err := db.CreateTable("users", []string{"id", "name", "age"})
	assert.NoError(t, err)

	// 尝试查询表
	var table Table
	key := "table:users"
	value, err := db.kv.Get([]byte(key))
	assert.NoError(t, err)
	err = json.Unmarshal(value, &table)
	assert.NoError(t, err)
	assert.Equal(t, "users", table.Name)
	assert.ElementsMatch(t, []string{"id", "name", "age"}, table.Columns)
}

func TestDropTable(t *testing.T) {
	db := setup()

	// 创建并删除一个表
	err := db.CreateTable("users", []string{"id", "name", "age"})
	assert.NoError(t, err)

	err = db.DropTable("users")
	assert.NoError(t, err)

	// 检查表是否删除
	key := "table:users"
	_, err = db.kv.Get([]byte(key))
	assert.Error(t, err) // 如果表删除成功，应该找不到此表
}

func TestInsertAndSelect(t *testing.T) {
	db := setup()

	// 创建表
	err := db.CreateTable("users", []string{"id", "name", "age"})
	assert.NoError(t, err)

	// 插入数据
	data := map[string]string{
		"id":   "1",
		"name": "Alice",
		"age":  "25",
	}
	err = db.Insert("users", data)
	assert.NoError(t, err)

	// 查询数据
	where := map[string]string{"id": "1"}
	results, err := db.Select("users", where)
	assert.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "Alice", results[0]["name"])
	assert.Equal(t, "25", results[0]["age"])
}

func TestUpdate(t *testing.T) {
	db := setup()

	// 创建表并插入数据
	err := db.CreateTable("users", []string{"id", "name", "age"})
	assert.NoError(t, err)

	data := map[string]string{
		"id":   "1",
		"name": "Alice",
		"age":  "25",
	}
	err = db.Insert("users", data)
	assert.NoError(t, err)

	// 更新数据
	updateData := map[string]string{
		"age": "26",
	}
	where := map[string]string{"id": "1"}
	err = db.Update("users", updateData, where)
	assert.NoError(t, err)

	// 验证更新
	results, err := db.Select("users", where)
	assert.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "26", results[0]["age"])
}

func TestDelete(t *testing.T) {
	db := setup()

	// 创建表并插入数据
	err := db.CreateTable("users", []string{"id", "name", "age"})
	assert.NoError(t, err)

	data := map[string]string{
		"id":   "1",
		"name": "Alice",
		"age":  "25",
	}
	err = db.Insert("users", data)
	assert.NoError(t, err)

	// 删除数据
	where := map[string]string{"id": "1"}
	err = db.Delete("users", where)
	assert.NoError(t, err)

	// 验证删除
	results, err := db.Select("users", where)
	assert.NoError(t, err)
	assert.Len(t, results, 0) // 数据应该被删除
}

// 测试插入20条数据并分页查询
func TestInsertAndSelectWithPagination(t *testing.T) {
	// 配置选项
	db := setup()

	assert.NotNil(t, db, "NewSQLDB should return a non-nil SQLDB instance")
	err := db.CreateTable("users", []string{"id", "name", "age"})
	assert.NoError(t, err)

	// 插入20条数据
	for i := 1; i <= 20; i++ {
		data := map[string]string{
			"id":   fmt.Sprintf("%d", i),
			"name": "Alice" + fmt.Sprintf("%d", i),
			"age":  "25",
		}
		err := db.Insert("users", data)
		assert.NoError(t, err, "Insert should not return an error")
	}

	// 分页查询数据
	pageSize := 10
	for page := 0; page < 2; page++ {
		results, err := db.SelectWithPagination("users", nil, pageSize, page*pageSize)
		assert.NoError(t, err, "SelectWithPagination should not return an error")

		// 打印查询结果
		fmt.Printf("Page %d results:\n", page+1)
		for _, record := range results {
			fmt.Println(record)
		}
	}
}
