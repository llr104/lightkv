package cache

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

func isExist(path string) bool {
	_, err := os.Stat(path) //os.Stat获取文件信息
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

func createDir(path string) error {
	if !isExist(path) {
		err := os.MkdirAll(path, os.ModePerm)
		if err != nil {
			return err
		}
		return err
	}
	return nil
}

type Item struct {
	Key   string
	Value value
}

type OpType int

const (
	Add = 0
	Del = 1
)

type persistentOp struct {
	item   Item
	opType OpType
}

type Cache struct{
	caches map[string]value
	checkExpireInterval int
	persistentChan chan persistentOp
	opFunction func(OpType, Item)
	mutex sync.RWMutex
}

const ExpireForever = 0
const DefaultDBPath = "db"

func NewCache(checkExpireInterval int) *Cache {
	 c := Cache{
	 	caches: make(map[string]value),
	 	checkExpireInterval:checkExpireInterval,
	 	persistentChan:make(chan persistentOp),
	 	opFunction:nil,
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

		 if data, err := ioutil.ReadFile(path); err != nil {
			 fmt.Println(err)
		 }else {
		 	 v := decode(data)
		 	 p := filepath.ToSlash(path)
		 	 arr := strings.Split(p, "/")
		 	 name := strings.Join(arr[1:], "/")
			 s.caches[name] = v
		 }

		return nil
	})

	fmt.Printf("load db finish, %d Key-cacheValue \n", len(s.caches))
}

func (s*Cache) SetOnOP(opFunc func(OpType, Item)) {
	s.opFunction = opFunc
}

func (s*Cache) Put(key string, v []byte, expire int64 ) {
	s.mutex.Lock()
	var val value
	if expire == ExpireForever {
		val = value{Data: v, Expire:ExpireForever}
		s.caches[key] = val

		item := Item{Key: key, Value:val}
		if s.opFunction != nil{
			s.opFunction(Add, item)
		}
	}else{
		e := time.Now().UnixNano() + expire*int64(time.Second)
		val = value{Data: v, Expire:e}
		s.caches[key] = val
		item := Item{Key: key, Value:val}
		if s.opFunction != nil{
			s.opFunction(Add, item)
		}
	}
	s.mutex.Unlock()

	fmt.Printf("put Key:%s, Value:%v, expire:%d\n", key, v, expire)

	item := Item{Key: key, Value:val}
	op := persistentOp{item:item, opType:Add}
	s.persistentChan <- op

}

func (s *Cache) Get(key string) ([]byte, bool) {
	s.mutex.RLock()
	v, ok := s.caches[key]
	s.mutex.RUnlock()
	if ok{
		t := time.Now().UnixNano()
		if v.Expire != ExpireForever && v.Expire <= t{
			fmt.Printf("get Key:%s, not found \n", key)
			return nil, false
		}else{
			fmt.Printf("get Key:%s, Value: %v \n", key, v.Data)
			return v.Data, true
		}
	}else{
		fmt.Printf("get Key:%s, not found \n", key)
		return nil, false
	}
}

func (s *Cache) Delete (key string) {
	s.mutex.Lock()
	s.del(key)
	s.mutex.Unlock()
}

func (s *Cache) del(key string) {

	fmt.Printf("del Key:%s\n", key)
	_, ok := s.caches[key]
	if ok{
		delete(s.caches, key)
		val := value{Expire: ExpireForever, Data:nil}
		item := Item{Key: key, Value:val}
		op := persistentOp{item:item, opType:Del}
		s.persistentChan <- op

		if s.opFunction != nil{
			s.opFunction(Add, item)
		}
	}


}

func (s *Cache) checkExpire() {
	for {
		time.Sleep(time.Duration(s.checkExpireInterval) * time.Second)
		s.mutex.Lock()
		t := time.Now().UnixNano()
		for k, v := range s.caches  {
			if v.Expire != ExpireForever && v.Expire <= t{
				s.del(k)
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
					s.saveKV(op.item.Key, op.item.Value)
				}else if op.opType == Del{
					s.delKV(op.item.Key)
				}
			}
	}

}

func (s *Cache) saveKV(key string, v value) {
	b := encode(v)

	fullPath := filepath.Join(DefaultDBPath, key)
	path, _ := filepath.Split(fullPath)

	createDir(path)

	err := ioutil.WriteFile(fullPath, b, os.ModePerm)
	if err != nil{
		fmt.Printf("saveKV error:%s\n", err.Error())
	}
}

func (s *Cache) delKV(key string)  {
	fullPath := filepath.Join(DefaultDBPath, key)
	os.Remove(fullPath)
}

func (s *Cache) All() map[string]value {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.caches
}

