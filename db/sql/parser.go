package sql

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// SQLParser 是一个简单的 SQL 解析器
type SQLParser struct {
	db *SQLDB
}

// NewSQLParser 创建一个新的 SQLParser 实例
func NewSQLParser(db *SQLDB) *SQLParser {
	return &SQLParser{db: db}
}

// ExecuteSQL 解析并执行 SQL 语句
func (p *SQLParser) ExecuteSQL(query string) (string, error) {
	query = strings.TrimSpace(query)
	command := strings.ToUpper(strings.Split(query, " ")[0])

	switch command {
	case "CREATE":
		return p.parseCreateTable(query)
	case "INSERT":
		return p.parseInsert(query)
	case "SELECT":
		return p.parseSelect(query)
	case "UPDATE":
		return p.parseUpdate(query)
	case "DELETE":
		return p.parseDelete(query)
	default:
		return "", errors.New("unknown command")
	}
}

//////////////////////////
// 1. CREATE TABLE 解析器
//////////////////////////

func (p *SQLParser) parseCreateTable(query string) (string, error) {
	// 匹配 CREATE TABLE 语句，支持多行格式和字段类型
	re := regexp.MustCompile(`(?i)CREATE TABLE (\w+)\s*\(([\s\S]+)\)`)
	matches := re.FindStringSubmatch(query)

	if len(matches) != 3 {
		return "", errors.New("invalid CREATE TABLE syntax")
	}

	tableName := matches[1]
	columnDefs := matches[2]
	columns := []string{}

	// 解析字段定义并去除多余的空格和换行
	columnLines := strings.Split(columnDefs, ",")
	for _, columnLine := range columnLines {
		column := strings.TrimSpace(columnLine)
		if column == "" {
			continue
		}
		// 提取字段名，忽略类型
		columnParts := strings.Fields(column)
		if len(columnParts) < 1 {
			return "", fmt.Errorf("invalid column definition: %s", column)
		}
		columns = append(columns, columnParts[0]) // 只保存字段名
	}

	err := p.db.CreateTable(tableName, columns)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Table '%s' created successfully.", tableName), nil
}

// ////////////////////////
// 2. INSERT 解析器
// ////////////////////////
func (p *SQLParser) parseInsert(query string) (string, error) {
	// 增强正则表达式处理多余空格和引号
	re := regexp.MustCompile(`(?i)INSERT INTO (\w+)\s*\((.+?)\)\s*VALUES\s*\((.+?)\)`)
	matches := re.FindStringSubmatch(query)

	if len(matches) != 4 {
		return "", errors.New("invalid INSERT syntax")
	}

	// 表名
	tableName := matches[1]
	// 列名
	columns := strings.Split(matches[2], ",")
	// 值
	values := strings.Split(matches[3], ",")

	if len(columns) != len(values) {
		return "", errors.New("columns and values count mismatch")
	}

	// 去掉每列和每个值的空格，并处理引号
	data := make(map[string]string)
	for i := range columns {
		column := strings.TrimSpace(columns[i])
		// 去掉引号，并处理空格
		value := strings.Trim(strings.TrimSpace(values[i]), "'\"")

		// 防止 SQL 注入，可以进一步验证数据的合法性
		data[column] = value
	}

	// 调用数据库插入方法
	err := p.db.Insert(tableName, data)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Row inserted into table '%s'.", tableName), nil
}

// ////////////////////////
// 3. SELECT 解析器
// ////////////////////////

func (p *SQLParser) parseSelect(query string) (string, error) {
	// 清除查询语句中的多余空格
	query = strings.TrimSpace(query)

	// 使用正则表达式匹配查询模式
	re := regexp.MustCompile(`(?i)^SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?$`)
	matches := re.FindStringSubmatch(query)

	if len(matches) < 3 {
		return "", errors.New("invalid SQL query: must start with SELECT and contain a table name")
	}

	// 获取查询的列和表名
	columnsPart := matches[1]
	tableName := matches[2]
	whereClause := make(map[string]string)

	// 如果有 WHERE 子句，解析它
	if len(matches) == 4 && matches[3] != "" {
		conditions := strings.Split(matches[3], " AND ")
		for _, condition := range conditions {
			parts := strings.Split(condition, "=")
			if len(parts) != 2 {
				return "", fmt.Errorf("invalid WHERE clause: expected column=value but got %s", condition)
			}
			column := strings.TrimSpace(parts[0])
			value := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
			whereClause[column] = value
		}
	}

	// 调用 Select 方法，获取结果
	results, err := p.db.Select(tableName, whereClause)
	if err != nil {
		return "", err
	}

	// 如果没有结果，返回对应信息
	if len(results) == 0 {
		return fmt.Sprintf("No results found for table '%s'.", tableName), nil
	}

	// 如果查询的是 *，获取所有列
	if columnsPart == "*" {
		// 获取第一行的所有列名
		var columnsList []string
		if len(results) > 0 {
			for column := range results[0] {
				columnsList = append(columnsList, column)
			}
		}
		columnsPart = strings.Join(columnsList, ", ")
	}

	// 格式化输出，只返回查询的列
	resultStr := fmt.Sprintf("Results from table '%s' (columns: %s):\n", tableName, columnsPart)
	columnsList := strings.Split(columnsPart, ",")
	for _, row := range results {
		for _, column := range columnsList {
			column = strings.TrimSpace(column)
			if value, exists := row[column]; exists {
				resultStr += fmt.Sprintf("%s: %v ", column, value)
			} else {
				resultStr += fmt.Sprintf("%s: (not found) ", column)
			}
		}
		resultStr += "\n"
	}

	return resultStr, nil
}

//////////////////////////
// 4. UPDATE 解析器
//////////////////////////

func (p *SQLParser) parseUpdate(query string) (string, error) {
	// 匹配 UPDATE table_name SET column1 = 'value1' WHERE column2 = 'value2'
	re := regexp.MustCompile(`(?i)UPDATE (\w+) SET (.+) WHERE (.+)`)
	matches := re.FindStringSubmatch(query)

	if len(matches) != 4 {
		return "", errors.New("invalid UPDATE syntax")
	}

	tableName := matches[1]
	setClause := matches[2]
	whereClauseStr := matches[3]

	setData := make(map[string]string)
	for _, setPair := range strings.Split(setClause, ",") {
		parts := strings.Split(setPair, "=")
		if len(parts) != 2 {
			return "", errors.New("invalid SET clause")
		}
		column := strings.TrimSpace(parts[0])
		value := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
		setData[column] = value
	}

	whereClause := make(map[string]string)
	conditions := strings.Split(whereClauseStr, " AND ")
	for _, condition := range conditions {
		parts := strings.Split(condition, "=")
		if len(parts) != 2 {
			return "", errors.New("invalid WHERE clause")
		}
		column := strings.TrimSpace(parts[0])
		value := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
		whereClause[column] = value
	}

	err := p.db.Update(tableName, setData, whereClause)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Table '%s' updated successfully.", tableName), nil
}

//////////////////////////
// 5. DELETE 解析器
//////////////////////////

func (p *SQLParser) parseDelete(query string) (string, error) {
	// 匹配 DELETE FROM table_name WHERE column1 = 'value1'
	re := regexp.MustCompile(`(?i)DELETE FROM (\w+) WHERE (.+)`)
	matches := re.FindStringSubmatch(query)

	if len(matches) != 3 {
		return "", errors.New("invalid DELETE syntax")
	}

	tableName := matches[1]
	whereClauseStr := matches[2]

	whereClause := make(map[string]string)
	conditions := strings.Split(whereClauseStr, " AND ")
	for _, condition := range conditions {
		parts := strings.Split(condition, "=")
		if len(parts) != 2 {
			return "", errors.New("invalid WHERE clause")
		}
		column := strings.TrimSpace(parts[0])
		value := strings.Trim(strings.TrimSpace(parts[1]), "'\"")
		whereClause[column] = value
	}

	err := p.db.Delete(tableName, whereClause)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Row(s) deleted from table '%s'.", tableName), nil
}
