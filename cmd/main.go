package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/sidneychang/no-db/config"
	"github.com/sidneychang/no-db/db/engine"
)

func main1() {
	// 配置选项
	options := config.NewOptions(1, 1024, os.TempDir())
	// 创建数据库实例
	db, err := engine.NewDB(*options)
	if err != nil {
		panic("fail to new db")
	}

	// 创建一个 scanner 来读取用户输入
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("Welcome to the NO-DB CLI!")
	fmt.Println("Type 'exit' to quit.")

	// 一直运行，直到用户输入 'exit'
	for {
		fmt.Print("Enter command: ")

		// 读取用户输入
		scanner.Scan()
		input := scanner.Text()

		// 将输入拆分成命令和参数
		parts := strings.Fields(input)
		if len(parts) == 0 {
			continue
		}

		// 处理 'exit' 命令来退出循环
		if parts[0] == "exit" {
			fmt.Println("Exiting...")
			break
		}

		// 根据输入的命令执行相应的操作
		switch parts[0] {
		case "put":
			if len(parts) != 3 {
				fmt.Println("Usage: put <key> <value>")
				continue
			}
			key := parts[1]
			value := parts[2]
			db.Put([]byte(key), []byte(value))
			fmt.Printf("Put: %s = %s\n", key, value)

		case "get":
			if len(parts) != 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			key := parts[1]
			val, err := db.Get([]byte(key))
			if err != nil {
				fmt.Println(err.Error())
				fmt.Printf("Key '%s' not found\n", key)
			} else {
				fmt.Printf("Get: %s = %v\n", key, string(val))
			}

		case "delete":
			if len(parts) != 2 {
				fmt.Println("Usage: delete <key>")
				continue
			}
			key := parts[1]
			db.Delete([]byte(key))
			fmt.Printf("Deleted: %s\n", key)

		default:
			fmt.Println("Unknown command:", parts[0])
			fmt.Println("Available commands: put, get, delete, exit")
		}
	}
}
