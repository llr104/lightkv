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

type Cache struct{
	caches map[string]value
	checkExpireInterval int
	mutex sync.RWMutex
}

const ExpireForever = 0
const DefaultDBPath = "db"

func NewCache(checkExpireInterval int) *Cache {
	 c := Cache{caches: make(map[string]value), checkExpireInterval:checkExpireInterval}
	 c.init()
	 return &c
}

func (s*Cache) init() {
	os.Mkdir(DefaultDBPath, os.ModePerm)
	s.loadDB()

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
			var v value
			json.Unmarshal(data, &v)
			s.caches[f.Name()] = v
		}
		return nil
	})

	fmt.Printf("load db finish, %d key-value \n", len(s.caches))
}

func (s*Cache) Put(key string, v interface{}, expire int64 ) {
	s.mutex.Lock()
	var val value
	if expire == ExpireForever {
		val = value{Data: v, Expire:ExpireForever}
		s.caches[key] = val
	}else{
		e := time.Now().UnixNano() + expire*int64(time.Second)
		val = value{Data: v, Expire:e}
		s.caches[key] = val
	}
	s.mutex.Unlock()
	s.saveKV(key, val)

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
	delete(s.caches, key)
	s.mutex.Unlock()

	s.delKV(key)
}

func (s *Cache) checkExpire() {
	for {
		time.Sleep(time.Duration(s.checkExpireInterval) * time.Second)
		s.mutex.Lock()
		t := time.Now().UnixNano()
		for k, v := range s.caches  {
			if v.Expire != ExpireForever && v.Expire <= t{
				delete(s.caches, k)
				s.delKV(k)
				//fmt.Printf("checkExpire delete: %s\n", k)
			}
		}
		s.mutex.Unlock()
	}
}

func (s *Cache) saveKV(key string, v value) {

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

func (s *Cache) All() map[string]value{
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.caches
}
