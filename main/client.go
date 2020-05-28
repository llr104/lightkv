
package main

import (
	"golang.org/x/net/context"
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

	go func() {
		t1 := bridge.NewRpcBridgeClient(conn)
		for {
			time.Sleep(1*time.Second/2)
			
			tr1, err := t1.Ping(context.Background(), &bridge.PingReq{Timestamp:0})
			if err != nil {
				log.Fatalf("could not greet: %v", err)
			}
			log.Printf("服务端响应: %d", tr1.Timestamp)
		}
	}()

	select {
	}

}
