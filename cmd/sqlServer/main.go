package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/sidneychang/no-db/config"
	"github.com/sidneychang/no-db/db/sql"
)

// SQLServer 是一个简单的 SQL 服务端
type SQLServer struct {
	parser *sql.SQLParser
}

// NewSQLServer 创建一个新的 SQLServer
func NewSQLServer(parser *sql.SQLParser) *SQLServer {
	return &SQLServer{parser: parser}
}

// HandleSQLRequest 处理 SQL 请求
func (s *SQLServer) HandleSQLRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	query := string(body)
	result, err := s.parser.ExecuteSQL(query)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error: %s", err.Error()), http.StatusBadRequest)
		return
	}

	response := map[string]string{
		"result": result,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func main() {
	// 初始化数据库和 SQL 解析器
	options := config.NewOptions(1, 1024, os.TempDir())
	// 创建数据库实例
	db, err := sql.NewSQLDB(*options)
	if err != nil {
		panic("fail to new db")
	}

	parser := sql.NewSQLParser(db) // 创建SQL解析器

	server := NewSQLServer(parser) // 创建服务端

	http.HandleFunc("/execute", server.HandleSQLRequest)

	fmt.Println("Starting SQL Server on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
