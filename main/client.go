
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

	//添加kv
	c.Put("test","test_value",0)
	c.Put("test1/tttt","test1_value",0)
	c.Put("test2","test2_value",20)

	//删除kv
	c.Del("test")

	v := c.Get("test2")
	fmt.Printf("test2:%s\n", v)

	//监听key值，发生变化回调通知
	c.WatchKey("watch1", func(k string, v string, t cache.OpType) {
		fmt.Printf("watch key:%s, value:%s, type:%d\n", k, v, t)
	})

	//key值发生变化回调通知
	c.WatchKey("unwatch", func(k string, v string, t cache.OpType) {
		fmt.Printf("watch key:%s, value:%s, type:%d\n", k, v, t)
	})

	time.Sleep(time.Second*20)
	//取消监听key值的变化
	c.UnWatchKey("unwatch")

	time.Sleep(time.Second*10)
	c.Close()
}
