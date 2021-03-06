
package main

import (
	"fmt"
	_ "fmt"
	"github.com/llr104/lightkv/cache/kv"
	"github.com/llr104/lightkv/server"
	"log"
	"time"
)

func main() {

	testString()
	testMap()
	testList()
	testSet()

	time.Sleep(time.Second*60)
}

func testString()  {
	c := server.NewClient("127.0.0.1:9980")
	c.Start()
	defer c.Close()

	c.ClearValue()

	//添加kv
	c.Put("test","test_value",0)
	c.Put("test1/tttt","test1_value",0)
	c.Put("test2","test2_value",5)
	c.Put("test3","test3_value",5)

	v := c.Get("test2")
	log.Printf("获取 test2 的值:%s", v)

	//删除kv
	c.Del("test2")
	log.Printf("删除 test2 之后的值:%s", c.Get("test2"))

	//监听key值，发生变化回调通知
	c.WatchKey("watch1", func(k string, beforeV string, afterV string, t kv.OpType) {
		if t == kv.Add {
			log.Printf("监听的 key:%s 新增了, 变化前:%s\n变化后:%s\n", k, beforeV, afterV)
		}else if t == kv.Del {
			log.Printf("监听的 key:%s 删除了, 变化前:%s\n变化后:%s\n", k, beforeV, afterV)
		}
	})

	//key值发生变化回调通知
	c.WatchKey("unwatch", func(k string, beforeV string, afterV string, t kv.OpType) {
		if t == kv.Add {
			log.Printf("监听的 key:%s 新增了, 变化前:%s\n变化后:%s\n", k, beforeV, afterV)
		}else if t == kv.Del {
			log.Printf("监听的 key:%s 删除了, 变化前:%s\n变化后:%s\n", k, beforeV, afterV)
		}
	})

	c.WatchKey("watchdel", func(k string, beforeV string, afterV string, t kv.OpType) {
		if t == kv.Add {
			log.Printf("监听的 key:%s 新增了, 变化前:%s\n变化后:%s\n", k, beforeV, afterV)
		}else if t == kv.Del {
			log.Printf("监听的 key:%s 删除了, 变化前:%s\n变化后:%s\n", k, beforeV, afterV)
		}
	})


	c.Put("unwatch", "this is before unwatch", 0)

	time.Sleep(1*time.Second)
	//取消监听key值的变化
	c.UnWatchKey("unwatch")

	c.Put("watch1", "this is watch1", 0)
	c.Put("unwatch", "this is after unwatch", 0)
	c.Put("watchdel", "this is watchdel", 0)

	log.Printf("获取 watchdel:%s", c.Get("watchdel"))

	c.Del("watchdel")

	log.Printf("获取 unwatch:%s", c.Get("unwatch"))

	time.Sleep(3*time.Second)

}


func testMap()  {
	c := server.NewClient("127.0.0.1:9980")
	c.Start()
	defer c.Close()

	c.ClearMap()

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


	c.HMWatch("hmtest2", "", func(hk string, k string, beforeV string, afterV string, t kv.OpType) {
		if k == ""{
			if t == kv.Add {
				log.Printf("监听的 map key:%s, 新增了, 变化前:%s\n变化后:%s\n", hk, beforeV, afterV)
			}else if t == kv.Del {
				log.Printf("监听的 map key:%s, 删除了, 变化前:%s\n变化后:%s\n", hk, beforeV, afterV)
			}
		}else{
			if t == kv.Add {
				log.Printf("监听的 map key:%s, 元素:%s, 新增了, 变化前:%s\n变化后:%s\n", hk, k, beforeV, afterV)
			}else if t == kv.Del {
				log.Printf("监听的 map key:%s, 元素:%s, 删除了，变化前:%s\n变化后:%s\n", hk, k, beforeV, afterV)
			}
		}
	})

	c.HMWatch("hmtest2", "k1", func(hk string, k string, beforeV string, afterV string, t kv.OpType) {
		if k == ""{
			if t == kv.Add {
				log.Printf("监听的 map key:%s, 新增了, 变化前:%s\n变化后:%s\n", hk, beforeV, afterV)
			}else if t == kv.Del {
				log.Printf("监听的 map key:%s, 删除了, 变化前:%s\n变化后:%s\n", hk, beforeV, afterV)
			}
		}else{
			if t == kv.Add {
				log.Printf("监听的 map key:%s, 元素:%s, 新增了, 变化前:%s\n变化后值为:\n%s", hk, k, beforeV, afterV)
			}else if t == kv.Del {
				log.Printf("监听的 map key:%s, 元素:%s, 删除了, 变化前:%s\n变化后值为:\n%s", hk, k, beforeV, afterV)
			}
		}
	})

	c.HMWatch("hmtest2", "", func(hk string, k string, beforeV string, afterV string, t kv.OpType) {
		if k == ""{
			if t == kv.Add {
				log.Printf("监听的 map key:%s, 新增了, 变化前:%s\n变化后:%s\n", hk, beforeV, afterV)
			}else if t == kv.Del {
				log.Printf("监听的 map key:%s, 删除了, 变化前:%s\n变化后:%s\n", hk, beforeV, afterV)
			}
		}else{
			if t == kv.Add {
				log.Printf("监听的 map key:%s, 元素:%s, 新增了, 变化前:%s\n变化后:%s\n", hk, k, beforeV, afterV)
			}else if t == kv.Del {
				log.Printf("监听的 map key:%s, 元素:%s, 删除了, 变化前:%s\n变化后:%s\n", hk, k, beforeV, afterV)
			}
		}

	})


	log.Printf("新增hmtest2 map")
	//新增hmtest2 map
	c.HMPut("hmtest2", keys, vals, 3)

	log.Printf("获取 hmtest2 map的值:\n%s", c.HMGet("hmtest2"))

	log.Printf("获取hmtest2 map 中k2元素:%s", c.HMGetMember("hmtest2", "k2"))

	//删除hmtest2 中的k2
	log.Printf("删除hmtest2 map 中k1元素")
	c.HMDelMember("hmtest2", "k1")

	c.HMPut("hmtest3", keys, vals, 0)
	log.Printf("获取hmtest2 map 中k1元素:%s", c.HMGetMember("hmtest2", "k1"))

	for i:=0; i<1000; i++{
		keys := []string{"k1", "k2", "k3"}
		vals := []string{"vddddddddddddddd1", "vgagdaga2", "v3gdsgaaaagagdag"}

		k := fmt.Sprintf("lru%d", i)
		//新增map
		c.HMPut(k, keys, vals, 0)
	}
	time.Sleep(6*time.Second)

}

func testList()  {
	c := server.NewClient("127.0.0.1:9980")
	c.Start()
	defer c.Close()

	c.ClearList()

	c.LPut("testlist", []string{"1","2", "3"}, 10)

	c.LPut("list1", []string{"a1","a2", "a3"}, 0)
	arr, _ := c.LGet("list1")
	log.Printf("获取list1:%v", arr)

	c.LPut("list2", []string{"b1","b2", "b3", "b4", "b5"}, 0)
	arr, _ = c.LGet("list2")
	log.Printf("获取list2:%v", arr)

	arr, _ = c.LGetRange("list2", 0,2)
	log.Printf("获取list2 0-2:元素%v", arr)

	log.Printf("删除list2 1-3位元素")
	c.LDelRange("list2", 1,3)

	arr, _ = c.LGet("list2")
	log.Printf("获取list2:%v", arr)

	c.LWatchKey("watchList", func(k string, beforeV []string, afterV []string, opType kv.OpType) {
		if opType == kv.Add {
			log.Printf("监听 %s 新增了, 变化前:%v\n变化后:%v\n", k, beforeV, afterV)
		}else{
			log.Printf("监听 %s 删除了, 变化前:%v\n变化后:%v\n", k, beforeV, afterV)
		}
	})

	log.Printf("添加watchList")
	c.LPut("watchList", []string{"c1", "c2", "c3", "c4", "c5"}, 0)

	log.Printf("删除watchList的0-2元素")
	c.LDelRange("watchList", 0,2)

	log.Printf("取消监听watchList")
	c.LUnWatchKey("watchList")

	log.Printf("删除watchList")
	c.LDel("watchList")

	arr, _ = c.LGet("watchList")
	log.Printf("获取watchList:%v", arr)

	time.Sleep(2*time.Second)

}

func testSet()  {
	c := server.NewClient("127.0.0.1:9980")
	c.Start()
	defer c.Close()

	c.ClearSet()

	c.SPut("set1", []string{"aset","bset", "cset", "aset"}, 0)

	arr, _:= c.SGet("set1")
	log.Printf("获取set1:%v", arr)

	arr, _= c.SGet("set2")
	log.Printf("在没有存入set2时获取set2:%v", arr)

	c.SPut("set2", []string{"aset2","bset2", "cset2", "aset2"}, 0)

	arr, _= c.SGet("set2")
	log.Printf("存入set2后获取set2:%v", arr)

	log.Printf("删除set2的cset2元素")
	c.SDelMember("set2", "cset2")

	arr, _= c.SGet("set2")
	log.Printf("获取set2:%v", arr)

	log.Printf("删除set1")
	c.SDel("set1")

	arr, _ = c.SGet("set1")
	log.Printf("获取set1:%v", arr)

	c.SWatchKey("setwatch", func(key string, before []string, after []string, opType kv.OpType) {
		if opType == kv.Add {
			log.Printf("监听 %s 新增了，新增前的值为：%v\n新增后的值为:%v\n", key, before, after)
		}else{
			log.Printf("监听 %s 删除了，删除前的值为：%v\n删除后的值为:%v\n", key, before, after)
		}
	})

	c.SPut("setwatch", []string{"setwatch1","setwatch2", "setwatc3", "setwatch4"}, 0)

	c.SDelMember("setwatch", "setwatch1")

	c.SDel("setwatch")

	time.Sleep(2*time.Second)
}