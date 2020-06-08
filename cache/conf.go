package cache

import (
	"gopkg.in/ini.v1"
	"log"
	"path"
)

var DefaultDBPath = "db"
var DefaultHost = ":9980"
var DefaultCheckExpireInterval = 15

var Conf config

type config struct {
	ValueDBPath         string
	MapDBPath           string
	ListDBPath          string
	SetDBPath           string
	Host                string
	CheckExpireInterval int
	CacheMaxSize		int
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

		host := cfg.Section("").Key("host").String()
		if host != ""{
			DefaultHost = host
		}


		if checkExpireInterval, err := cfg.Section("").Key("checkExpireInterval").Int(); err == nil{
			DefaultCheckExpireInterval = checkExpireInterval
		}

		if cacheMaxSize, err := cfg.Section("").Key("cacheMaxSize").Int(); err == nil{
			Conf.CacheMaxSize = cacheMaxSize * (1024*1024)
		}else{
			Conf.CacheMaxSize = 1024 * (1024*1024) //1GB
		}
	}

	Conf.ValueDBPath = path.Join(DefaultDBPath, "value")
	Conf.MapDBPath = path.Join(DefaultDBPath, "map")
	Conf.ListDBPath = path.Join(DefaultDBPath, "list")
	Conf.SetDBPath = path.Join(DefaultDBPath, "set")
	Conf.Host = DefaultHost
	Conf.CheckExpireInterval = DefaultCheckExpireInterval

}