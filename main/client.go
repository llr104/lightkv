
package main

import (
	_ "fmt"
	"github.com/llr104/lightkv/cache"
	"github.com/llr104/lightkv/server"
	"log"
	"time"
)

func main() {

	c := server.NewClient()
	c.Start()

	//添加kv
	c.Put("test","test_value",0)
	c.Put("test1/tttt","test1_value",0)
	c.Put("test2","test2_value",20)


	v := c.Get("test2")
	log.Printf("before del test2:%s\n", v)

	//删除kv
	c.Del("test2")
	log.Printf("after del test2:%s\n", c.Get("test2"))

	//监听key值，发生变化回调通知
	c.WatchKey("watch1", func(k string, v string, t cache.OpType) {
		log.Printf("watch key:%s, value:%s, type:%d\n", k, v, t)
	})

	//key值发生变化回调通知
	c.WatchKey("unwatch", func(k string, v string, t cache.OpType) {
		log.Printf("watch key:%s, value:%s, type:%d\n", k, v, t)
	})

	time.Sleep(time.Second*3)

	c.Put("unwatch", "this is before unwatch", 0)
	//取消监听key值的变化
	c.UnWatchKey("unwatch")

	c.Put("watch1", "this is watch1", 0)
	c.Put("unwatch", "this is after unwatch", 0)

	log.Printf("after :%s\n", c.Get("unwatch"))

	keys := []string{"k1", "k2", "k3"}
	vals := []string{"v1", "v2", "v3"}

	c.HMPut("hmtest1", keys, vals, 0)

	time.Sleep(time.Second*3)
	str := c.HMGet("hmtest1")
	log.Printf("hmtest1 before del k1:%s\n", str)

	c.HMDelMember("hmtest1", "k1")
	str = c.HMGet("hmtest1")
	log.Printf("hmtest1 after del k1:%s\n", str)

	log.Printf("before del hmtest1:%s\n", c.HMGet("hmtest1"))
	c.HMDel("hmtest1")
	log.Printf("after del hmtest1:%s\n", c.HMGet("hmtest1"))
	time.Sleep(time.Second*1)

	c.HMWatch("hmtest2", "", func(k string, v string, t cache.OpType) {
		log.Printf("watch key:%s, value:%s, type:%d\n", k, v, t)
	})

	c.HMWatch("hmtest2", "k1", func(k string, v string, t cache.OpType) {
		log.Printf("hmtest2 1111 watch key:%s, value:%s, type:%d\n", k, v, t)
	})

	c.HMWatch("hmtest2", "", func(k string, v string, t cache.OpType) {
		log.Printf("hmtest2 2222 watch key:%s, value:%s, type:%d\n", k, v, t)
	})


	c.HMPut("hmtest2", keys, vals, 50)

	time.Sleep(time.Second*60)
	c.Close()
	
}
