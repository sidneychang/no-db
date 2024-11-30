package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"

	"github.com/sidneychang/no-db/config"
	"github.com/sidneychang/no-db/db/engine"
	pb "github.com/sidneychang/no-db/proto" // 替换为你的 protobuf 路径

	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedKVDBServer
	mu             sync.Mutex
	db             *engine.DB
	primaryAddr    string          // 主节点地址（仅副本节点使用）
	isPrimary      bool            // 是否是 Primary
	replicaClients []pb.KVDBClient // 仅 Primary 节点使用
}

// Put 方法：客户端写请求
func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. 将数据写入本地存储
	err := s.db.Put([]byte(req.Key), []byte(req.Value))
	if err != nil {
		return nil, err
	}
	log.Printf("[%s] Put %s = %s\n", s.getRole(), req.Key, req.Value)

	// 2. 如果是 Primary，则将写操作复制到副本
	if s.isPrimary {
		go s.replicateToReplicas(req)
	}

	return &pb.Empty{}, nil
}

// Get 方法：客户端读请求
func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	value, err := s.db.Get([]byte(req.Key))
	if err != nil {
		log.Printf("Get failed for key: %s, error: %v\n", req.Key, err)
		return nil, err
	}

	log.Printf("[%s] Get %s = %s\n", s.getRole(), req.Key, string(value))
	return &pb.GetResponse{Value: string(value)}, nil
}

// Delete 方法：客户端删除请求
func (s *server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.Empty, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 如果是 Primary，处理删除并同步到副本
	if s.isPrimary {
		// 删除本地数据
		err := s.db.Delete([]byte(req.Key))
		if err != nil {
			return nil, err
		}
		log.Printf("[%s] Deleted key: %s\n", s.getRole(), req.Key)

		// 如果是 Primary，则将删除操作复制到副本
		go s.replicateDeleteToReplicas(req)

	} else {
		err := s.db.Delete([]byte(req.Key))
		if err != nil {
			return nil, err
		}
		// 如果是 Replica，直接删除
		log.Printf("[%s] Deleted key: %s\n", s.getRole(), req.Key)
	}

	return &pb.Empty{}, nil
}

// 将删除操作复制到所有副本
func (s *server) replicateDeleteToReplicas(req *pb.DeleteRequest) {
	var wg sync.WaitGroup
	for _, replica := range s.replicaClients {
		wg.Add(1)
		go func(replica pb.KVDBClient) {
			defer wg.Done()
			_, err := replica.Delete(context.Background(), req)
			if err != nil {
				log.Printf("Error replicating delete to replica: %v\n", err)
			}
		}(replica)
	}
	wg.Wait()
}

// 将写操作复制到所有副本
func (s *server) replicateToReplicas(req *pb.PutRequest) {
	var wg sync.WaitGroup
	for _, replica := range s.replicaClients {
		wg.Add(1)
		go func(replica pb.KVDBClient) {
			defer wg.Done()
			_, err := replica.Put(context.Background(), req)
			if err != nil {
				log.Printf("Error replicating to replica: %v\n", err)
			}
		}(replica)
	}
	wg.Wait()
}

// 获取当前节点的角色
func (s *server) getRole() string {
	if s.isPrimary {
		return "Primary"
	}
	return "Replica"
}

func main() {
	// 解析启动参数 --role 和 --replicas
	isPrimary := flag.Bool("primary", false, "Run as primary server")
	replicas := flag.String("replicas", "", "Comma-separated list of replica addresses")
	pathdir := flag.String("pathdir", os.TempDir(), "The directory for data storage")
	port := flag.Int("port", 50051, "Server port")
	flag.Parse()

	// 初始化 Server
	s, err := NewServer(*pathdir, *isPrimary)
	if err != nil {
		log.Fatalf("Failed to initialize server: %v", err)
	}

	// 如果是 Primary，则初始化副本客户端连接
	if s.isPrimary && *replicas != "" {
		s.initReplicaClients(*replicas)
	}

	// 启动 gRPC Server
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Failed to listen on port %d: %v", *port, err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterKVDBServer(grpcServer, s)

	log.Printf("[%s] Server listening on port %d", s.getRole(), *port)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

// 初始化副本连接
func (s *server) initReplicaClients(replicaAddrs string) {
	addrs := strings.Split(replicaAddrs, ",")
	for _, addr := range addrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Failed to connect to replica: %v", err)
		}
		client := pb.NewKVDBClient(conn)
		s.replicaClients = append(s.replicaClients, client)
	}
}
func NewServer(pathdir string, isPrimary bool) (*server, error) {
	// 使用指定的 pathdir 作为数据存储目录
	options := config.NewOptions(1, 1024, pathdir)
	db, err := engine.NewDB(*options)
	if err != nil {
		return nil, err
	}
	return &server{db: db, isPrimary: isPrimary}, nil
}
