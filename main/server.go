package main

import (
	"github.com/llr104/lightkv/cache"
	"github.com/llr104/lightkv/server"
)

func main() {
	c := cache.NewCache(15)
	api := server.NewApi(c)
	go api.Start()

	server.NewRpcServer(c)
}
