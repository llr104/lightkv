package server

import (
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
	"lightkv/cache"
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

type server struct{
	cache *cache.Cache
}

func (s *server) Ping(ctx context.Context, in *bridge.PingReq) (*bridge.PingRsp, error) {
	return &bridge.PingRsp{Timestamp:time.Now().Unix()}, nil
}

func (s *server) Get(ctx context.Context, in *bridge.GetReq) (*bridge.GetRsp, error) {
	v, ok := s.cache.Get(in.Key)
	if ok {
		return &bridge.GetRsp{Key:in.Key, Value:v}, nil
	}else{
		return &bridge.GetRsp{Key:in.Key, Value:nil}, nil
	}
}

func (s *server) Put(ctx context.Context, in *bridge.PutReq) (*bridge.PutRsp, error) {
	s.cache.Put(in.Key, in.Value, in.Expire)
	return &bridge.PutRsp{Key:in.Key, Value:in.Value}, nil
}

func (s *server) Del(ctx context.Context, in *bridge.DelReq) (*bridge.DelRsp, error) {
	s.cache.Delete(in.Key)
	return &bridge.DelRsp{Key:in.Key}, nil
}

func NewRpcServer(cache *cache.Cache)  {
	listen, err := net.Listen("tcp", ":9980")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	ser := server{cache:cache}
	s := grpc.NewServer(grpc.StatsHandler(&rpcHandler{}))
	bridge.RegisterRpcBridgeServer(s, &ser)
	s.Serve(listen)

}

