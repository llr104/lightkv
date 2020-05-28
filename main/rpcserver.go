package main

import (
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
	"lightkv/pb"
	"net"
	"time"
)

type rpcHandler struct {

}

func (s* rpcHandler) TagRPC(ctx context.Context, info*stats.RPCTagInfo) context.Context {
	//fmt.Println("TagRPC")
	return ctx
}

func (s* rpcHandler) HandleRPC(ctx context.Context, stat stats.RPCStats)  {
	//fmt.Println("HandleRPC")
}

func (s* rpcHandler) TagConn(ctx context.Context, stat *stats.ConnTagInfo) context.Context {
	fmt.Println("TagConn")
	return ctx
}

func (s* rpcHandler) HandleConn(ctx context.Context, stat stats.ConnStats)  {
	//fmt.Println("HandleConn")
}

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
	s := grpc.NewServer(grpc.StatsHandler(&rpcHandler{}))

	bridge.RegisterRpcBridgeServer(s, &server{})

	s.Serve(listen)
}
