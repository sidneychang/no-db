package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
)

// sendSQLRequest 发送 SQL 请求到服务端
func sendSQLRequest(query string) (string, error) {
	url := "http://localhost:8080/execute"
	body := bytes.NewBufferString(query)

	resp, err := http.Post(url, "text/plain", body)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errorMsg, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("server error: %s", string(errorMsg))
	}

	var result map[string]string
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return "", err
	}

	return result["result"], nil
}

func main() {
	fmt.Println("SQL Client - Type 'exit' to quit")

	// 使用 bufio.Scanner 来读取完整的用户输入
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("sql> ")
		scanner.Scan()
		query := scanner.Text()

		if query == "exit" {
			break
		}

		result, err := sendSQLRequest(query)
		if err != nil {
			fmt.Println("Error:", err)
			continue
		}

		fmt.Println(result)
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading input:", err)
	}
}
