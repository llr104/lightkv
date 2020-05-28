
package main

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"lightkv/pb"
	"log"
)


func main() {

	conn, err := grpc.Dial("127.0.0.1:9980", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	defer conn.Close()

	t1 := bridge.NewRpcBridgeClient(conn)
	tr1, err := t1.Ping(context.Background(), &bridge.PingReq{Timestamp:0})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	log.Printf("服务端响应: %d", tr1.Timestamp)
}
