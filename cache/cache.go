package cache

import (
	"sync"
	"time"
)

type Cache struct{
	caches map[string]value
	checkExpireInterval int
	mutex sync.RWMutex
}

const ExpireForever = 0

func NewCache(checkExpireInterval int) *Cache {
	 c := Cache{caches: make(map[string]value), checkExpireInterval:checkExpireInterval}
	 go c.checkExpire()
	 return &c
}

func (s*Cache) Put(key string, v interface{}, expire int64 ) {
	s.mutex.Lock()
	if expire == ExpireForever {
		s.caches[key] = value{Data: v, Expire:ExpireForever}
	}else{
		e := time.Now().UnixNano() + expire*int64(time.Second)
		s.caches[key] = value{Data: v, Expire:e}
	}
	s.mutex.Unlock()
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
}

func (s *Cache) checkExpire() {
	for {
		time.Sleep(time.Duration(s.checkExpireInterval) * time.Second)
		s.mutex.Lock()
		t := time.Now().UnixNano()
		for k, v := range s.caches  {
			if v.Expire != ExpireForever && v.Expire <= t{
				delete(s.caches, k)
				//fmt.Printf("checkExpire delete: %s\n", k)
			}
		}
		s.mutex.Unlock()
	}
}

func (s *Cache) All() map[string]value{
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.caches
}
