
package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"lightkv/pb"
	"log"
	"time"
)


func main() {

	conn, err := grpc.Dial("127.0.0.1:9980", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	defer conn.Close()

	t1 := bridge.NewRpcBridgeClient(conn)


	go func() {

		for {
			time.Sleep(1*time.Second/2)
			_, err := t1.Ping(context.Background(), &bridge.PingReq{Timestamp:0})
			if err != nil {
				log.Fatalf("could not greet: %v", err)
			}
		}
	}()


	r, _ := t1.Get(context.Background(), &bridge.GetReq{Key:"k1"})
	log.Printf("get k1:%v\n",r)

	t1.Put(context.Background(), &bridge.PutReq{Key:"keyRpc",  Value:"rpcPush", Expire:0})

	t1.WatchKey(context.Background(), &bridge.WatchReq{Key:"watch1"})
	t1.WatchKey(context.Background(), &bridge.WatchReq{Key:"watch2"})
	t1.UnWatchKey(context.Background(), &bridge.WatchReq{Key:"watch2"})

	//服务端 客户端 双向流
	p, err := t1.Publish(context.Background())
	if err != nil{
		fmt.Println(err)
	}

	go func() {
		for{
			time.Sleep(time.Second)
			p.Send(&bridge.PublishReq{Key:"client"})
		}
	}()

	go func() {
		for {
			data, err := p.Recv()
			if err != nil{
				//log.Printf("err:%s\n", err.Error())
			}else{
				log.Printf("%s,%s,%d\n", data.Key, data.Value, data.Type)
			}
			time.Sleep(time.Second*1)
		}
	}()



	select {
	}

}
