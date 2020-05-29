package main

import (
	"lightkv/cache"
	"lightkv/server"
)

func main() {
	c := cache.NewCache(15)
	api := server.NewApi(c)
	go api.Start()

	server.NewRpcServer(c)
}
