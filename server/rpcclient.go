package server

import (
	"github.com/llr104/lightkv/cache"
	bridge "github.com/llr104/lightkv/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"sync"
	"time"
)

type rpcClient struct {
	c        bridge.RpcBridgeClient
	conn     *grpc.ClientConn
	mutex    sync.Mutex
	mapMutex    sync.Mutex
	watchKey map[string]func(string, string, cache.OpType)
	watchMap map[string]map[string]func(string, string, cache.OpType)
}

func NewClient() *rpcClient{

	s := rpcClient{watchKey: make(map[string]func(string, string, cache.OpType)),
		watchMap:make(map[string]map[string]func(string, string, cache.OpType))}

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
		log.Println(err)
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
					log.Printf("data.HmKey: %s,data.Key: %s,data.Value: %s,data.Type: %d\n", data.HmKey, data.Key, data.Value, data.Type)
					if data.HmKey == ""{
						s.mutex.Lock()
						f, ok := s.watchKey[data.Key]
						if ok {
							f(data.Key, data.Value, cache.OpType(data.Type))
						}
						s.mutex.Unlock()
					}else{
						s.mapMutex.Lock()
						m, ok := s.watchMap[data.HmKey]
						if ok {
							f, ok1 := m[data.Key]
							if ok1{
								if data.Key == ""{
									f(data.HmKey, data.Value, cache.OpType(data.Type))
								}else{
									f(data.Key, data.Value, cache.OpType(data.Type))
								}
							}
						}
						s.mapMutex.Unlock()
					}

				}
				time.Sleep(time.Second/100)
			}
		}()
	}
}

func (s*rpcClient) Close()  {
	s.conn.Close()
}

func (s*rpcClient) Put(key string, value string, expire int64) error{
	_, err := s.c.Put(context.Background(), &bridge.PutReq{Key:key,  Value:value, Expire:expire})
	if err != nil{
		log.Printf("Put error: %s\n", err.Error())
	}
	return err
}

func (s*rpcClient) Get(key string) string{
	rsp, err := s.c.Get(context.Background(), &bridge.GetReq{Key: key})
	if err == nil{
		return rsp.Value
	}else{
		log.Printf("Get error: %s\n", err.Error())
		return ""
	}

}

func (s*rpcClient) Del(key string) error{
	_, err := s.c.Del(context.Background(), &bridge.DelReq{Key: key})
	if err != nil{
		log.Printf("Del error: %s\n", err.Error())
	}
	return err
}


func (s *rpcClient) HMGet(hmKey string) string {
	rsp, err := s.c.HMGet(context.Background(), &bridge.HMGetReq{HmKey:hmKey})
	if err != nil{
		log.Printf("HMGet error: %s\n", err.Error())
	}
	return rsp.GetValue()
}

func (s *rpcClient) HMGetMember(hmKey string, key string) string{
	rsp, err := s.c.HMGetMember(context.Background(), &bridge.HMGetMemberReq{HmKey:hmKey, Key:key})

	if err != nil{
		log.Printf("HMGetMember error: %s\n", err.Error())
	}
	return rsp.GetValue()
}

func (s *rpcClient) HMPut(hmKey string, key []string, val [] string, expire int64) error{
	_, err := s.c.HMPut(context.Background(), &bridge.HMPutReq{HmKey:hmKey, Key:key, Value:val, Expire:expire})
	if err != nil{
		log.Printf("HMGet error: %s\n", err.Error())
	}
	return err
}

func (s *rpcClient) HMDel(hmKey string)  error {
	_, err := s.c.HMDel(context.Background(), &bridge.HMDelReq{HmKey: hmKey})
	return err
}

func (s *rpcClient) HMDelMember(hmKey string, key string) error {
	_, err := s.c.HMDelMember(context.Background(), &bridge.HMDelMemberReq{HmKey:hmKey, Key:key})
	return err
}

func (s*rpcClient) WatchKey(key string, watchFunc func(string, string, cache.OpType)) error{
	_, err := s.c.WatchKey(context.Background(), &bridge.WatchReq{Key: key})
	if err != nil{
		log.Printf("WatchKey error: %s\n", err.Error())
	}else{
		s.mutex.Lock()
		s.watchKey[key] = watchFunc
		s.mutex.Unlock()
	}
	return err
}

func (s*rpcClient) UnWatchKey(key string) error{
	_, err := s.c.UnWatchKey(context.Background(), &bridge.WatchReq{Key: key})
	if err != nil{
		log.Printf("UnWatchKey error: %s\n", err.Error())
	}else{
		s.mutex.Lock()
		delete(s.watchKey, key)
		s.mutex.Unlock()
	}
	return err
}


func (s *rpcClient) HMWatch(hmKey string, key string, watchFunc func(string, string, cache.OpType)) error {
	_, err := s.c.HMWatch(context.Background(), &bridge.HMWatchReq{HmKey:hmKey, Key:key})
	if err != nil{
		log.Printf("HMWatch error: %s\n", err.Error())
	}else{
		s.mapMutex.Lock()
		m, ok := s.watchMap[hmKey]
		if !ok {
			m = make(map[string] func(string, string, cache.OpType))
		}
		m[key] = watchFunc
		s.watchMap[hmKey] = m
		s.mapMutex.Unlock()
	}

	return err
}

func (s *rpcClient) HMUnWatchHM(hmKey string, key string) error{
	_, err := s.c.HMWatch(context.Background(), &bridge.HMWatchReq{HmKey:hmKey, Key:key})

	if err != nil{
		log.Printf("HMUnWatchHM error: %s\n", err.Error())
	}else{
		s.mapMutex.Lock()
		m, ok := s.watchMap[hmKey]
		if ok {
			delete(m, key)
		}
		s.watchMap[hmKey] = m
		s.mapMutex.Unlock()
	}
	return err
}



