package cache

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/llr104/lightkv/cache/kv"
	"sync"
	"time"
)

type expireTrigger  func(key string, v kv.ValueCache)

type lru struct {
	cacheType 		int32
	l				*list.List
	caches 			map[string]*list.Element
	cacheSize 		int
	rwMutex 		sync.RWMutex
	expireTrigger   expireTrigger
}

func newLRU(cacheType int32, trigger expireTrigger) *lru{
	return &lru{
		cacheType:		cacheType,
		l:              list.New(),
		caches:         make(map[string]*list.Element),
		expireTrigger:  trigger,
	}
}

func (s* lru) PushFront(v kv.ValueCache) {
	if v == nil{
		return
	}

	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()

	//删除原有的
	s.remove(v.GetKey())

	//添加
	e := s.l.PushFront(v)
	s.caches[v.GetKey()] = e

	s.cacheSize += v.Size()
}

func (s *lru) Value(key string) (kv.ValueCache, error) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()

	v, ok := s.caches[key]
	if ok{
		s.moveToFront(key)
		return v.Value.(kv.ValueCache), nil
	}else{
		str := fmt.Sprintf("data type: %d not have key:%s ValueCache", s.cacheType, key)
		return nil, errors.New(str)
	}
}

func (s *lru) Clear()  {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()

	s.l = list.New()
	s.caches = make(map[string]*list.Element)
	s.cacheSize = 0
}

func (s *lru) CacheToString() ([]byte, error)  {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()

	m := make(map[string]kv.ValueCache)
	for k,v:=range s.caches{
		m[k] = v.Value.(kv.ValueCache)
	}
	return json.MarshalIndent(m, "", "    ")
}

func (s *lru) Size() int{
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()

	return s.cacheSize
}

func (s* lru) Remove(key string) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.remove(key)
}

func (s* lru) SetExpireTrigger(trigger expireTrigger)  {
	s.expireTrigger = trigger
}

func (s* lru) moveToFront(key string) {
	v, ok := s.caches[key]
	if ok {
		s.l.MoveToFront(v)
	}
}

func (s* lru) remove(key string) {

	v, ok := s.caches[key]
	if ok {
		s.cacheSize -= v.Value.(kv.ValueCache).Size()
		s.l.Remove(v)
		delete(s.caches, key)
	}
}

func (s *lru) checkExpire() {
	for {
		time.Sleep(time.Duration(Conf.CheckExpireInterval) * time.Second)
		s.rwMutex.Lock()

		for k, v := range s.caches{
			ok := v.Value.(kv.ValueCache).IsExpire()
			if ok{
				if s.expireTrigger != nil{
					s.expireTrigger(k, v.Value.(kv.ValueCache))
				}
				s.remove(k)
			}
		}
		s.rwMutex.Unlock()
	}
}