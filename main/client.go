
package main

import (
	"fmt"
	_ "fmt"
	"lightkv/cache"
	"lightkv/server"
	"time"
)

func main() {

	c := server.NewClient()
	c.Start()

	c.Put("test","test_value",0)
	c.Put("test1","test1_value",0)
	c.Put("test2","test2_value",20)
	c.Del("test1")

	v := c.Get("test2")
	fmt.Printf("test2:%s\n", v)

	c.WatchKey("watch1", func(k string, v string, t cache.OpType) {
		fmt.Printf("watch key:%s, value:%s, type:%d\n", k, v, t)
	})

	c.WatchKey("unwatch", func(k string, v string, t cache.OpType) {
		fmt.Printf("watch key:%s, value:%s, type:%d\n", k, v, t)
	})

	time.Sleep(time.Second*10)
	c.UnWatchKey("unwatch")

	time.Sleep(time.Second*10)
	c.Close()
}
