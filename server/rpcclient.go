package server

import (
	"fmt"
	"github.com/llr104/lightkv/cache"
	bridge "github.com/llr104/lightkv/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"sync"
	"time"
)

type rpcClient struct {
	c bridge.RpcBridgeClient
	conn *grpc.ClientConn
	mutex sync.Mutex
	watchMap map[string]func(string, string, cache.OpType)
}

func NewClient() *rpcClient{

	s := rpcClient{watchMap:make(map[string]func(string, string, cache.OpType))}

	conn, err := grpc.Dial("127.0.0.1:9980", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	c := bridge.NewRpcBridgeClient(conn)
	s.c = c
	s.conn = conn

	return &s
}

func (s*rpcClient) Start() {
	go func() {
		for {
			time.Sleep(1*time.Second/2)
			_, err := s.c.Ping(context.Background(), &bridge.PingReq{Timestamp:time.Now().UnixNano()})
			if err != nil {
				log.Fatalf("could not greet: %v", err)
			}
		}
	}()


	//服务端 客户端 双向流
	if p, err := s.c.Publish(context.Background()); err != nil{
		fmt.Println(err)
	}else{
		go func() {
			for{
				time.Sleep(time.Second)
				p.Send(&bridge.PublishReq{Timestamp:time.Now().UnixNano()/int64(time.Millisecond)})
			}
		}()

		go func() {
			for {
				data, err := p.Recv()
				if err != nil{
					//log.Printf("err:%s\n", err.Error())
				}else{
					log.Printf("%s,%s,%d\n", data.Key, data.Value, data.Type)
					s.mutex.Lock()
					f, ok := s.watchMap[data.Key]
					if ok {
						f(data.Key, data.Value, cache.OpType(data.Type))
					}

					s.mutex.Unlock()
				}
				time.Sleep(time.Second/100)
			}
		}()
	}
}

func (s*rpcClient) Close()  {
	s.conn.Close()
}

func (s*rpcClient) Put(key string, value string, expire int64){
	_, err := s.c.Put(context.Background(), &bridge.PutReq{Key:key,  Value:value, Expire:expire})
	if err != nil{
		fmt.Printf("Put error: %s\n", err.Error())
	}
}

func (s*rpcClient) Get(key string) string{
	rsp, err := s.c.Get(context.Background(), &bridge.GetReq{Key: key})
	if err == nil{
		return rsp.Value
	}else{
		fmt.Printf("Get error: %s\n", err.Error())
		return ""
	}

}

func (s*rpcClient) Del(key string) {
	_, err := s.c.Del(context.Background(), &bridge.DelReq{Key: key})
	if err != nil{
		fmt.Printf("Del error: %s\n", err.Error())
	}
}

func (s*rpcClient) WatchKey(key string, watchFunc func(string, string, cache.OpType)) {
	_, err := s.c.WatchKey(context.Background(), &bridge.WatchReq{Key: key})
	if err != nil{
		fmt.Printf("WatchKey error: %s\n", err.Error())
	}else{
		s.mutex.Lock()
		s.watchMap[key] = watchFunc
		s.mutex.Unlock()
	}

}

func (s*rpcClient) UnWatchKey(key string) {
	_, err := s.c.UnWatchKey(context.Background(), &bridge.WatchReq{Key: key})
	if err != nil{
		fmt.Printf("UnWatchKey error: %s\n", err.Error())
	}

	s.mutex.Lock()
	delete(s.watchMap, key)
	s.mutex.Unlock()
}


