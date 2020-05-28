package main

import (
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"lightkv/pb"
	"net"
	"time"
)

type server struct{}


func (s *server) Ping(ctx context.Context, in *bridge.PingReq) (*bridge.PingRsp, error) {
	return &bridge.PingRsp{Timestamp:time.Now().Unix()}, nil
}

func main() {
	listen, err := net.Listen("tcp", ":9980")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	s := grpc.NewServer()
	bridge.RegisterRpcBridgeServer(s, &server{})

	s.Serve(listen)
}
