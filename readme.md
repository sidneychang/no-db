# NO-DB 

NO-DB 是一个简单的键值存储系统，使用 Go 和 gRPC 构建，支持基本的 CRUD 操作，并且实现了分布式部署、数据副本和负载均衡功能。该项目使用一致性哈希来决定数据存储位置，并提供了客户端和服务端的交互功能。

## 环境要求

- **Go 版本**: 1.22.9
- **gRPC**: 用于客户端与服务端之间的通信
- **Protobuf**: 用于定义 gRPC 服务

### 安装 Go 1.22.9

首先，请确保你已安装 Go 1.22.9 版本。你可以通过以下命令检查当前 Go 版本：

```bash
go version
```

如果输出的版本不是 1.22.9，请访问 [Go 下载页面](https://golang.org/dl/) 获取并安装该版本。

### 安装 gRPC 和 Protobuf

你需要安装 gRPC 库来实现客户端和服务端的通信：

```bash
go get google.golang.org/grpc
```

如果你还没有安装 `protoc`（Protobuf 编译器），请参考 [Protobuf 安装文档](https://grpc.io/docs/protoc-installation/) 来安装它。

## 项目结构

```
D:.
├── cmd
│   ├── client      # 客户端入口
│   └── server      # 服务器入口
├── config          # 配置文件目录
├── consistanthash  # 一致性哈希算法实现
├── db              # 数据存储相关
│   ├── data        # 存储数据的文件
│   ├── engine      # 数据存储引擎
│   ├── fileio      # 文件 I/O 相关操作
│   └── index       # 索引相关操作
├── proto           # Protobuf 文件（gRPC 服务定义）
└── README.md       # 项目的说明文档
```

### 目录说明

- **cmd/client**: 客户端的入口文件，提供 CLI 命令行工具与服务端交互。
- **cmd/server**: 服务端的入口文件，启动 gRPC 服务并处理客户端请求。
- **config**: 配置文件目录，包含项目运行时的配置选项。
- **consistanthash**: 实现了一致性哈希算法，用于分布式系统中的负载均衡。
- **db**:
  - **data**: 存储数据的目录。
  - **engine**: 数据存储引擎的核心实现。
  - **fileio**: 处理文件读取和写入的模块。
  - **index**: 负责数据的索引处理。
- **proto**: 存放用于 gRPC 服务定义的 Protobuf 文件。

## 如何运行项目

### 1. 初始化 Go Modules

在项目的根目录下运行以下命令，初始化 Go Modules 并下载依赖：

```bash
go mod tidy
```

### 2. 运行服务端

服务端需要指定两个参数：`-port`（指定 gRPC 服务的端口）和 `-pathdir`（指定数据存储目录）。如果数据目录不存在，请手动创建，或者选择一个有效的路径。

#### 启动服务端

例如，运行以下命令启动服务端实例：

```bash
go run ./cmd/server/main.go -port=:50051 -pathdir=./db/data1
```

- `-port=:50051`：指定 gRPC 服务器监听的端口。
- `-pathdir=./db/data1`：指定数据存储目录。

#### 启动多个服务端实例

为了实现分布式部署和负载均衡，你可以启动多个服务端实例。每个实例应该使用不同的端口和数据存储路径，并且在client/main.go中指定哈希环中得到ip。例如：
```go
hashRing = consistanthash.NewHashRing([]string{"0.0.0.0:50051", "0.0.0.0:50052","0.0.0.0:50053","0.0.0.0:50054"})
```

```bash
go run ./cmd/server/main.go -port=:50051 -pathdir=./db/data1
go run ./cmd/server/main.go -port=:50052 -pathdir=./db/data2
go run ./cmd/server/main.go -port=:50053 -pathdir=./db/data3
go run ./cmd/server/main.go -port=:50054 -pathdir=./db/data4
```

- `50051`、`50052`、`50053` 和 `50054` 分别是不同的服务端端口。
- `./db/data1`、`./db/data2` 等是不同的存储路径。

#### 服务器输出示例：

```bash
2024/11/27 13:24:21 gRPC server running on port :50052 with data stored in ./db/data1
```

### 3. 运行客户端

客户端是一个命令行工具，允许用户与服务端进行交互，执行键值对的增、删、查操作。

#### 启动客户端

运行以下命令启动客户端：

```bash
go run ./cmd/client/main.go
```

#### 客户端命令

客户端支持以下命令：

- `put <key> <value>`: 向数据库中添加一个键值对。
- `get <key>`: 获取指定键的值。
- `delete <key>`: 删除指定键及其值。
- `exit`: 退出客户端。

#### 客户端示例：

```bash
Welcome to the NO-DB CLI!
Available commands: put <key> <value>, get <key>, delete <key>, exit
Enter command: put a ihiuh
Put: a = ihiuh (and replica)

Enter command: put jhihui uihih
Put: jhihui = uihih (and replica)

Enter command: get a
Get: a = ihiuh
```

### 4. 配置一致性哈希

项目中使用一致性哈希算法来进行负载均衡和高可用性。你可以在客户端代码中配置服务端实例的 IP 和端口。客户端会根据一致性哈希选择目标服务端来存储或获取数据。

例如，在客户端代码 `main.go` 中，你可以设置多个服务端实例的地址：

```go
hashRing = consistanthash.NewHashRing([]string{
	"192.168.1.2:50051",  // 第一个服务端实例
	"192.168.1.3:50051",  // 第二个服务端实例
	"192.168.1.4:50051",  // 第三个服务端实例
	"192.168.1.5:50051",  // 第四个服务端实例
})
```

客户端会使用一致性哈希算法来根据键值（key）选择对应的服务端。

### 5. 连接池管理

客户端实现了连接池机制，避免了频繁创建连接的开销。每当客户端选择服务端节点时，都会先检查连接池中是否已有该服务端的连接。如果有，直接使用现有连接；如果没有，则建立新的连接并加入连接池。

```go
func getClientConnection(key string) (pb.KVDBClient, error) {
	nodeAddr := hashRing.Get(key)  // 获取一致性哈希路由的目标服务端地址

	if conn, exists := connPool[nodeAddr]; exists {
		// 连接池中已有连接，直接返回
		return pb.NewKVDBClient(conn), nil
	}

	// 如果没有连接，则创建新的连接
	conn, err := grpc.Dial(nodeAddr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("Failed to connect to server: %v", err)
	}

	connPool[nodeAddr] = conn // 将新连接加入连接池
	return pb.NewKVDBClient(conn), nil
}
```

### 6. 配置动态添加或删除服务端节点(未实现)

在运行时，你可以动态添加或删除服务端节点。例如，添加一个新的服务端节点：


这会将新的节点添加到一致性哈希环中，之后的请求会根据新的哈希计算路由到该节点。

## 常见问题

### 1. 错误：`The system cannot find the file specified.`

这个错误通常是因为指定的数据目录不存在。请确保目录存在并且路径正确。如果目录不存在，可以手动创建目录：

```bash
mkdir ./db/data
```

然后再次运行服务端。

### 2. 错误：端口被占用

如果你遇到端口被占用的错误，请尝试更换端口。例如，将服务端的端口更改为 `50053`，同时需要在client目录下main.go中修改哈希环中的ip：

```bash
go run ./cmd/server/main.go -port=:50053 -pathdir=./db/data
```
```go
hashRing = consistanthash.NewHashRing([]string{"0.0.0.0:50051", "0.0.0.0:50053"})
```

### 3. 其他 gRPC 错误

如果你遇到与 gRPC 相关的错误，确保所有 Protobuf 文件都已经正确编译，并且服务端和客户端的接口定义一致。你可以使用以下命令重新生成 Protobuf 文件：

```bash
protoc --go_out=. --go-grpc_out=. proto/*.proto
```

### 4. 如何配置一致性哈希

项目中包括了一致性哈希算法（`consistanthash` 目录）。该算法可用于分布式存储的负

载均衡和高可用性配置。你可以在客户端的代码中设置不同的服务端地址来实现负载均衡。

