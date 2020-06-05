
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
	log.Printf("获取 test2 的值:%s", v)

	//删除kv
	c.Del("test2")
	log.Printf("删除 test2 之后的值:%s", c.Get("test2"))

	//监听key值，发生变化回调通知
	c.WatchKey("watch1", func(k string, v string, t cache.OpType) {
		if t == cache.Add{
			log.Printf("监听的 key:%s 新增了, 值为:%s", k, v)
		}else if t == cache.Del{
			log.Printf("监听的 key:%s 被删除了, 删除前值为:%s", k, v)
		}
	})

	//key值发生变化回调通知
	c.WatchKey("unwatch", func(k string, v string, t cache.OpType) {
		if t == cache.Add{
			log.Printf("监听的 key:%s 新增了, 值为:%s", k, v)
		}else if t == cache.Del{
			log.Printf("监听的 key:%s 被删除了, 删除前值为:%s", k, v)
		}
	})

	c.WatchKey("watchdel", func(k string, v string, t cache.OpType) {
		if t == cache.Add{
			log.Printf("监听的 key:%s 新增了, 值为:%s", k, v)
		}else if t == cache.Del{
			log.Printf("监听的 key:%s 被删除了, 删除前值为:%s", k, v)
		}
	})


	time.Sleep(time.Second*3)

	c.Put("unwatch", "this is before unwatch", 0)

	//取消监听key值的变化
	c.UnWatchKey("unwatch")

	c.Put("watch1", "this is watch1", 0)
	c.Put("unwatch", "this is after unwatch", 0)
	c.Put("watchdel", "this is watchdel", 0)

	c.Del("watchdel")

	log.Printf("获取 unwatch:%s", c.Get("unwatch"))

	keys := []string{"k1", "k2", "k3"}
	vals := []string{"v1", "v2", "v3"}

	//新增map
	c.HMPut("hmtest1", keys, vals, 0)

	str := c.HMGet("hmtest1")
	log.Printf("获取hmtest1 map:\n%s", str)

	//删除hmtest1 map 中的k1
	c.HMDelMember("hmtest1", "k1")
	str = c.HMGet("hmtest1")

	log.Printf("hmtest1 map 删除了k1后:\n%s", str)

	c.HMDel("hmtest1")
	log.Printf("删除hmtest1 map后，hmtest1的值:\n%s", c.HMGet("hmtest1"))


	c.HMWatch("hmtest2", "", func(hk string, k string, v string, t cache.OpType) {
		if k == ""{
			if t == cache.Add{
				log.Printf("监听的 map key:%s, 新增了, 值为:\n%s", hk, v)
			}else if t == cache.Del{
				log.Printf("监听的 map key:%s, 删除前值为:\n%s", hk, v)
			}
		}else{
			if t == cache.Add{
				log.Printf("监听的 map key:%s, 元素:%s, 新增了, 值为:\n%s", hk, k, v)
			}else if t == cache.Del{
				log.Printf("监听的 map key:%s, 元素:%s, 删除前值为:\n%s", hk, k, v)
			}
		}
	})

	c.HMWatch("hmtest2", "k1", func(hk string, k string, v string, t cache.OpType) {
		if k == ""{
			if t == cache.Add{
				log.Printf("监听的 map key:%s, 新增了, 值为:\n%s", hk, v)
			}else if t == cache.Del{
				log.Printf("监听的 map key:%s, 删除前值为:\n%s", hk, v)
			}
		}else{
			if t == cache.Add{
				log.Printf("监听的 map key:%s, 元素:%s, 新增了, 值为:\n%s", hk, k, v)
			}else if t == cache.Del{
				log.Printf("监听的 map key:%s, 元素:%s, 删除前值为:\n%s", hk, k, v)
			}
		}
	})

	c.HMWatch("hmtest2", "", func(hk string, k string, v string, t cache.OpType) {
		if k == ""{
			if t == cache.Add{
				log.Printf("监听的 map key:%s, 新增了, 值为:\n%s", hk, v)
			}else if t == cache.Del{
				log.Printf("监听的 map key:%s, 删除前值为:\n%s", hk, v)
			}
		}else{
			if t == cache.Add{
				log.Printf("监听的 map key:%s, 元素:%s, 新增了, 值为:\n%s", hk, k, v)
			}else if t == cache.Del{
				log.Printf("监听的 map key:%s, 元素:%s, 删除前值为:\n%s", hk, k, v)
			}
		}

	})


	log.Printf("新增hmtest2 map")
	//新增hmtest2 map
	c.HMPut("hmtest2", keys, vals, 20)

	log.Printf("获取 hmtest2 map的值:\n%s", c.HMGet("hmtest2"))


	log.Printf("获取hmtest2 map 中k2元素:%s", c.HMGetMember("hmtest2", "k2"))

	//删除hmtest2 中的k2
	log.Printf("删除hmtest2 map 中k2元素")
	c.HMDelMember("hmtest2", "k2")

	log.Printf("获取hmtest2 map 中k2元素:%s", c.HMGetMember("hmtest2", "k2"))

	time.Sleep(time.Second*60)
	c.Close()

}
