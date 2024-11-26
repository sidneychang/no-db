package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"

	pb "github.com/sidneychang/no-db/proto" // 替换为生成的 pb 文件的路径

	"google.golang.org/grpc"

	"github.com/sidneychang/no-db/config"
	"github.com/sidneychang/no-db/db/engine"
)

type server struct {
	pb.UnimplementedKVDBServer
	db *engine.DB
}

func NewServer(pathdir string) (*server, error) {
	// 使用指定的 pathdir 作为数据存储目录
	options := config.NewOptions(1, 1024, pathdir)
	db, err := engine.NewDB(*options)
	if err != nil {
		return nil, err
	}
	return &server{db: db}, nil
}

func (s *server) Put(ctx context.Context, req *pb.PutRequest) (*pb.Empty, error) {
	err := s.db.Put([]byte(req.Key), []byte(req.Value))
	if err != nil {
		return nil, err
	}
	log.Printf("Put: %s = %s\n", req.Key, req.Value)
	return &pb.Empty{}, nil
}

func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	value, err := s.db.Get([]byte(req.Key))
	if err != nil {
		log.Printf("Get failed for key: %s, error: %v\n", req.Key, err)
		return nil, err
	}
	log.Printf("Get: %s = %s\n", req.Key, string(value))
	return &pb.GetResponse{Value: string(value)}, nil
}

func (s *server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.Empty, error) {
	err := s.db.Delete([]byte(req.Key))
	if err != nil {
		return nil, err
	}
	log.Printf("Deleted: %s\n", req.Key)
	return &pb.Empty{}, nil
}

func main() {
	// 使用 flag 包从命令行接收端口号和存储路径
	port := flag.String("port", ":50051", "The server port")
	pathdir := flag.String("pathdir", os.TempDir(), "The directory for data storage")
	flag.Parse()

	lis, err := net.Listen("tcp", *port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	// 使用指定的 pathdir 初始化服务器
	s, err := NewServer(*pathdir)
	if err != nil {
		log.Fatalf("Failed to initialize server: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterKVDBServer(grpcServer, s)

	log.Printf("gRPC server running on port %s with data stored in %s", *port, *pathdir)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
