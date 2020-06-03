package server

import (
	"fmt"
	"github.com/llr104/lightkv/cache"
	"github.com/llr104/lightkv/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
	"net"
	"sync"
	"time"
)

type rpcHandler struct {
	mutex    sync.Mutex
	curID    uint16
	proxyMap map[string]*rpcProxy
}

func (s* rpcHandler) TagRPC(ctx context.Context, info*stats.RPCTagInfo) context.Context {
	//fmt.Println("TagRPC")
	return ctx
}

func (s* rpcHandler) HandleRPC(ctx context.Context, stat stats.RPCStats)  {

	/*
	fmt.Println("HandleRPC")

	switch stat.(type) {
	case *stats.Begin:
		fmt.Println("HandleRPC begin")
	case *stats.End:
		fmt.Println("HandleRPC end")
	}*/

}

func (s* rpcHandler) TagConn(ctx context.Context, stat *stats.ConnTagInfo) context.Context {
	fmt.Println("TagConn")
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.curID++
	tag := fmt.Sprintf("%d_timestamp_%d", s.curID, time.Now().UnixNano())
	if s.curID >2^15{
		s.curID = 0
	}
	s.proxyMap[tag] = &rpcProxy{watchKey:make(map[string]string)}
	fmt.Printf("TagConn:%s\n", tag)

	return context.WithValue(ctx, "curID", tag)
}

func (s* rpcHandler) HandleConn(ctx context.Context, stat stats.ConnStats)  {
	//fmt.Println("HandleConn")
	switch stat.(type) {
	case *stats.ConnBegin:
		fmt.Println("HandleConn begin")
		/*
		s.mutex.Lock()
		cid := ctx.value("curID")
		proxy, ok := s.proxyMap[cid.(string)]
		s.mutex.Unlock()
		 */

	case *stats.ConnEnd:
		fmt.Println("HandleConn end")

		s.mutex.Lock()
		cid := ctx.Value("curID")
		proxy, ok := s.proxyMap[cid.(string)]
		if ok{
			delete(s.proxyMap, cid.(string))
			proxy.sendCancel()
			proxy.recvCancel()
		}
		s.mutex.Unlock()

		fmt.Printf("remove proxy curID:%s\n", cid)
	}

}

func (s *rpcHandler) onOP(op cache.OpType, item cache.Item)  {
	//fmt.Printf("key onOP:%s\n", item.Key)
	s.mutex.Lock()
	for _, proxy := range s.proxyMap{
		_, ok := proxy.watchKey[item.Key]
		if ok {
			//通知推送
			rsp := bridge.PublishRsp{Key:item.Key, Value:item.Value.Data, Type:int32(op)}
			proxy.sendChan <- rsp
		}
	}
	s.mutex.Unlock()
}

type server struct{
	cache *cache.Cache
	handler *rpcHandler
}



func (s *server) Publish(p bridge.RpcBridge_PublishServer) error {

	s.handler.mutex.Lock()
	cid := p.Context().Value("curID")
	proxy, ok := s.handler.proxyMap[cid.(string)]
	s.handler.mutex.Unlock()

	if ok == false{
		return nil
	}else{

		wg := sync.WaitGroup{}
		wg.Add(2)

		s.recvLoop(proxy, p, &wg)
		s.sendLoop(proxy, p, &wg)

		wg.Wait()
	}

	return nil
}

func (s *server) recvLoop(proxy *rpcProxy, p bridge.RpcBridge_PublishServer, wg *sync.WaitGroup)  {
	ctx, cancel := context.WithCancel(context.Background())
	proxy.sendCancel = cancel

	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				goto end
			default:
				p.Recv()
				time.Sleep(time.Second/100)
			}
		}
	end:
		fmt.Println("Publish recv end")
		wg.Done()
	}(ctx)

}

func (s *server) sendLoop(proxy *rpcProxy, p bridge.RpcBridge_PublishServer, wg *sync.WaitGroup)  {
	ch := make(chan bridge.PublishRsp, 1024)
	ctx, cancel := context.WithCancel(context.Background())
	proxy.recvCancel = cancel
	proxy.sendChan = ch

	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				goto end
			case rsp := <-proxy.sendChan:
				fmt.Printf("send rsp: %v", rsp)
				p.Send(&rsp)
			}
		}
	end:
		fmt.Println("Publish send end")
		wg.Done()
	}(ctx)

}

func (s *server) Ping(ctx context.Context, in *bridge.PingReq) (*bridge.PingRsp, error) {
	return &bridge.PingRsp{Timestamp:time.Now().Unix()}, nil
}

func (s *server) Get(ctx context.Context, in *bridge.GetReq) (*bridge.GetRsp, error) {
	v, ok := s.cache.Get(in.Key)
	if ok {
		return &bridge.GetRsp{Key:in.Key, Value:v}, nil
	}else{
		return &bridge.GetRsp{Key:in.Key, Value:""}, nil
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

func (s *server) WatchKey(ctx context.Context, in *bridge.WatchReq) (*bridge.WatchRsp, error) {
	s.handler.mutex.Lock()
	cid := ctx.Value("curID")
	proxy, ok := s.handler.proxyMap[cid.(string)]
	if ok {
		proxy.watchKey[in.Key] = in.Key
	}

	s.handler.mutex.Unlock()

	return &bridge.WatchRsp{Key:in.Key}, nil
}

func (s *server) UnWatchKey(ctx context.Context, in *bridge.WatchReq) (*bridge.WatchRsp, error) {
	s.handler.mutex.Lock()
	cid := ctx.Value("curID")
	proxy, ok := s.handler.proxyMap[cid.(string)]
	if ok {
		delete(proxy.watchKey, in.Key)
	}
	s.handler.mutex.Unlock()

	return &bridge.WatchRsp{Key:in.Key}, nil
}


func NewRpcServer(cache *cache.Cache)  {
	listen, err := net.Listen("tcp", ":9980")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	handler := &rpcHandler{proxyMap: make(map[string]*rpcProxy), curID:0}
	ser := server{cache:cache, handler: handler}
	cache.SetOnOP(handler.onOP)
	s := grpc.NewServer(grpc.StatsHandler(handler))
	bridge.RegisterRpcBridgeServer(s, &ser)
	s.Serve(listen)

}

