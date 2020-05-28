package cache

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type cacheItem struct {
	key   string
	value cacheValue
}

type opType int

const (
	Add = 0
	Del = 1
)

type persistentOp struct {
	item   cacheItem
	opType opType
}

type Cache struct{
	caches map[string]cacheValue
	checkExpireInterval int
	persistentChan chan persistentOp
	mutex sync.RWMutex
}

const ExpireForever = 0
const DefaultDBPath = "db"

func NewCache(checkExpireInterval int) *Cache {
	 c := Cache{
	 	caches: make(map[string]cacheValue),
	 	checkExpireInterval:checkExpireInterval,
	 	persistentChan:make(chan persistentOp),
	 }
	 c.init()
	 return &c
}

func (s*Cache) init() {
	os.Mkdir(DefaultDBPath, os.ModePerm)
	s.loadDB()

	go s.persistent()
	go s.checkExpire()
}

func (s *Cache) loadDB()  {

	 filepath.Walk(DefaultDBPath, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() {
			return nil
		}

		if data, err := ioutil.ReadFile(path); err == nil {
			var v cacheValue
			json.Unmarshal(data, &v)
			s.caches[f.Name()] = v
		}
		return nil
	})

	fmt.Printf("load db finish, %d key-cacheValue \n", len(s.caches))
}

func (s*Cache) Put(key string, v interface{}, expire int64 ) {
	s.mutex.Lock()
	var val cacheValue
	if expire == ExpireForever {
		val = cacheValue{Data: v, Expire:ExpireForever}
		s.caches[key] = val
	}else{
		e := time.Now().UnixNano() + expire*int64(time.Second)
		val = cacheValue{Data: v, Expire:e}
		s.caches[key] = val
	}
	s.mutex.Unlock()

	item := cacheItem{key:key, value:val}
	op := persistentOp{item:item, opType:Add}
	s.persistentChan <- op

}

func (s *Cache) Get(key string) (interface{}, bool) {
	s.mutex.RLock()
	v, ok := s.caches[key]
	s.mutex.RUnlock()
	if ok{
		t := time.Now().UnixNano()
		if v.Expire != ExpireForever && v.Expire <= t{
			return nil, false
		}else{
			return v.Data, true
		}
	}else{
		return nil, false
	}
}

func (s *Cache) Delete (key string) {
	s.mutex.Lock()
	s.del(key)
	s.mutex.Unlock()
}

func (s *Cache) del(key string) {
	delete(s.caches, key)
	item := cacheItem{key:key, value:cacheValue{Expire:ExpireForever, Data:nil}}
	op := persistentOp{item:item, opType:Del}
	s.persistentChan <- op
}

func (s *Cache) checkExpire() {
	for {
		time.Sleep(time.Duration(s.checkExpireInterval) * time.Second)
		s.mutex.Lock()
		t := time.Now().UnixNano()
		for k, v := range s.caches  {
			if v.Expire != ExpireForever && v.Expire <= t{
				s.del(k)
				//fmt.Printf("checkExpire delete: %s\n", k)
			}
		}
		s.mutex.Unlock()
	}
}

func (s *Cache) persistent()  {
	for{
		select {
			case op := <-s.persistentChan:
				if op.opType == Add {
					s.saveKV(op.item.key, op.item.value)
				}else if op.opType == Del{
					s.delKV(op.item.key)
				}
			}
	}

}

func (s *Cache) saveKV(key string, v cacheValue) {

	data, err := json.Marshal(v)
	if err == nil{
		err := ioutil.WriteFile(DefaultDBPath +"/" + key, data, os.ModePerm)
		if err != nil{
			fmt.Println(err)
		}
	}
}

func (s *Cache) delKV(key string)  {
	os.Remove(DefaultDBPath +"/" + key)
}

func (s *Cache) All() map[string]cacheValue {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.caches
}
