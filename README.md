# lightkv 轻量化key-value服务

## 启动server
```bash
go run main/server.go  
```
  
  
- 默认会启动一个api(http://localhost:9981)服和一个rpc服(9980端口)

- http://localhost:9981/put?key=add1&value=addvalue1 api新增一条kv，key为add1,value为addvalue1，kv不过期 

- http://localhost:9981/put?key=add2&value=addvalue2&expire=100 api新增一条kv，key为add2,value为addvalue2，100秒后kv过期

- http://localhost:9981/del/add2 api删除key为add2的kv

- http://localhost:9981/get/add1 api获取key为add1的kv



## 启动测试rpc客户端
```bash
  go run main/client.go  
```
  


## rpc 客户端用法

```go
c := server.NewClient()
c.Start()

//添加kv
c.Put("test", "test_value", 0)
c.Put("test1/tttt", "test1_value", 0)
c.Put("test2", "test2_value", 20)

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

//取消监听key值的变化  
c.UnWatchKey("unwatch")  
c.Close()
```