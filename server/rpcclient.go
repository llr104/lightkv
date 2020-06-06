package server

import (
	"encoding/json"
	"github.com/llr104/lightkv/cache"
	bridge "github.com/llr104/lightkv/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"sync"
	"time"
)

type WatchKeyFunc func(string, string, string, cache.OpType)
type WatchMapFunc func(string, string, string, string, cache.OpType)
type WatchListFunc func(string, string, string, cache.OpType)

type rpcClient struct {
	c          bridge.RpcBridgeClient
	conn       *grpc.ClientConn
	valueMutex sync.Mutex
	mapMutex   sync.Mutex
	listMutex  sync.Mutex

	watchKey 	map[string]WatchKeyFunc
	watchMap 	map[string]map[string]WatchMapFunc
	watchList 	map[string]WatchListFunc

}

func NewClient() *rpcClient{

	s := rpcClient{watchKey: make(map[string]WatchKeyFunc),
		watchMap:make(map[string]map[string]WatchMapFunc),
		watchList:make(map[string]WatchListFunc)}

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

					if data.DataType == cache.ValueData{

						s.valueMutex.Lock()
						f, ok := s.watchKey[data.Key]
						if ok {
							f(data.Key, data.BeforeValue, data.AfterValue, cache.OpType(data.Type))
						}
						s.valueMutex.Unlock()

					}else if data.DataType == cache.MapData {

						s.mapMutex.Lock()
						m, ok := s.watchMap[data.HmKey]
						if ok {

							afterMap := make(map[string]string)
							if e := json.Unmarshal([]byte(data.AfterValue), &afterMap); e != nil{
								return
							}

							beforeMap := make(map[string]string)
							if e := json.Unmarshal([]byte(data.BeforeValue), &beforeMap); e != nil{
								return
							}

							//按key值回调,变化了的key才会回调
							for k, f := range m {
								if k == ""{
									continue
								}

								bv, _ := beforeMap[k]
								av, _ := afterMap[k]

								if bv != av{
									f(data.HmKey, k, bv, av, cache.OpType(data.Type))
								}
							}

							//全量回调
							if f, ok := m[""]; ok{
								f(data.HmKey, "", data.BeforeValue, data.AfterValue, cache.OpType(data.Type))
							}
						}
						s.mapMutex.Unlock()

					}else if data.DataType == cache.ListData{

						s.listMutex.Lock()
						f, ok := s.watchList[data.Key]
						if ok {
							f(data.Key, data.BeforeValue, data.AfterValue, cache.OpType(data.Type))
						}
						s.listMutex.Unlock()
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

func (s*rpcClient) WatchKey(key string, watchFunc func(string, string, string, cache.OpType)) error{
	_, err := s.c.WatchKey(context.Background(), &bridge.WatchReq{Key: key})
	if err != nil{
		log.Printf("WatchKey error: %s\n", err.Error())
	}else{
		s.valueMutex.Lock()
		s.watchKey[key] = watchFunc
		s.valueMutex.Unlock()
	}
	return err
}

func (s*rpcClient) UnWatchKey(key string) error{
	_, err := s.c.UnWatchKey(context.Background(), &bridge.WatchReq{Key: key})
	if err != nil{
		log.Printf("UnWatchKey error: %s\n", err.Error())
	}else{
		s.valueMutex.Lock()
		delete(s.watchKey, key)
		s.valueMutex.Unlock()
	}
	return err
}

/*
hmKey: map key
key:元素key， 空为监听整个map
*/
func (s *rpcClient) HMWatch(hmKey string, key string, watchFunc WatchMapFunc) error {
	_, err := s.c.HMWatch(context.Background(), &bridge.HMWatchReq{HmKey:hmKey, Key:key})
	if err != nil{
		log.Printf("HMWatch error: %s\n", err.Error())
	}else{
		s.mapMutex.Lock()
		m, ok := s.watchMap[hmKey]
		if !ok {
			m = make(map[string] WatchMapFunc)
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



