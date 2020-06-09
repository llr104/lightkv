package cache

import (
	"gopkg.in/ini.v1"
	"log"
	"path"
)

var DefaultDBPath = "db"
var DefaultRpcHost = ":9980"
var DefaultApiHost = ":9981"
var DefaultCheckExpireInterval = 15

var Conf config

type config struct {
	ValueDBPath         string
	MapDBPath           string
	ListDBPath          string
	SetDBPath           string
	RpcHost             string
	ApiHost				string
	CheckExpireInterval int
	CacheValueSize      int
	CacheMapSize        int
	CacheListSize       int
	CacheSetSize        int

}

func init() {

	Conf = config{}
	cfg, err := ini.Load("conf/kv.ini")
	if err != nil{
		log.Printf("no conf/kv.ini conf, use default")
	}else{
		dbPath := cfg.Section("").Key("dbPath").String()
		if dbPath != ""{
			DefaultDBPath = dbPath
		}

		rpcHost := cfg.Section("").Key("rpcHost").String()
		if rpcHost != ""{
			DefaultRpcHost = rpcHost
		}

		apiHost := cfg.Section("").Key("apiHost").String()
		if apiHost != ""{
			DefaultApiHost = apiHost
		}


		if checkExpireInterval, err := cfg.Section("").Key("checkExpireInterval").Int(); err == nil{
			DefaultCheckExpireInterval = checkExpireInterval
		}

		if cacheValueSize, err := cfg.Section("").Key("cacheValueSize").Int(); err == nil{
			Conf.CacheValueSize = cacheValueSize * (1024*1024)
		}else{
			Conf.CacheMapSize = 500 * (1024*1024) //500M
		}

		if cacheMapSize, err := cfg.Section("").Key("cacheMapSize").Int(); err == nil{
			Conf.CacheMapSize = cacheMapSize * (1024*1024)
		}else{
			Conf.CacheMapSize = 500 * (1024*1024) //500M
		}

		if cacheListSize, err := cfg.Section("").Key("cacheListSize").Int(); err == nil{
			Conf.CacheListSize = cacheListSize * (1024*1024)
		}else{
			Conf.CacheListSize = 500 * (1024*1024) //500M
		}

		if cacheSetSize, err := cfg.Section("").Key("cacheSetSize").Int(); err == nil{
			Conf.CacheSetSize = cacheSetSize * (1024*1024)
		}else{
			Conf.CacheSetSize = 500 * (1024*1024) //500M
		}
	}

	Conf.ValueDBPath = path.Join(DefaultDBPath, "kv")
	Conf.MapDBPath = path.Join(DefaultDBPath, "map")
	Conf.ListDBPath = path.Join(DefaultDBPath, "list")
	Conf.SetDBPath = path.Join(DefaultDBPath, "set")
	Conf.RpcHost = DefaultRpcHost
	Conf.ApiHost = DefaultApiHost
	Conf.CheckExpireInterval = DefaultCheckExpireInterval

}