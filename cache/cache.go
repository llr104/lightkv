package cache

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
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



type Cache struct{
	valueCaches map[string]Value
	mapCaches   map[string]MapValue
	listCaches  map[string]ListValue
	setCaches  	map[string]SetValue

	persistentChan chan persistentValueOp
	persistentMapChan chan persistentMapOp
	persistentListChan chan persistentListOp
	persistentSetChan chan persistentSetOp

	valueMutex sync.RWMutex
	mapMutex   sync.RWMutex
	listMutex  sync.RWMutex
	setMutex   sync.RWMutex

	valueCachesSize int
	mapCachesSize 	int
	listCachesSize 	int
	setCachesSize 	int

	opFunction func(OpType, ValueCache, ValueCache)

}

const ExpireForever = 0


func NewCache() *Cache {
	 c := Cache{
	 	valueCaches:         make(map[string]Value),
	 	mapCaches:           make(map[string]MapValue),
	 	listCaches:          make(map[string]ListValue),
	 	setCaches:           make(map[string]SetValue),
	 	persistentChan:      make(chan persistentValueOp),
	 	persistentMapChan:   make(chan persistentMapOp),
	 	persistentListChan:  make(chan persistentListOp),
	 	persistentSetChan:   make(chan persistentSetOp),
	 	opFunction:          nil,
	 }
	 c.init()
	 return &c
}

func (s*Cache) init() {

	createDir(Conf.ValueDBPath)
	createDir(Conf.MapDBPath)
	createDir(Conf.ListDBPath)
	createDir(Conf.SetDBPath)

	s.loadDB()

	go s.persistent()
	go s.checkExpire()
}

func (s *Cache) loadDB()  {

	 //普通类型
	 filepath.Walk(Conf.ValueDBPath, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() {
			return nil
		}

		 if data, err := ioutil.ReadFile(path); err != nil {
			 log.Println(err)
		 }else {
		 	 v := decodeValue(data)
			 s.valueCaches[v.Key] = v
			 s.valueCachesSize += v.Size()
		 }
		return nil
	})

	//map类型
	filepath.Walk(Conf.MapDBPath, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() {
			return nil
		}

		if data, err := ioutil.ReadFile(path); err != nil {
			log.Println(err)
		}else {
			v := decodeHM(data)
			s.mapCaches[v.Key] = v
			s.mapCachesSize += v.Size()
		}
		return nil
	})

	//list类型
	filepath.Walk(Conf.ListDBPath, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() {
			return nil
		}

		if data, err := ioutil.ReadFile(path); err != nil {
			log.Println(err)
		}else {
			v := decodeList(data)
			s.listCaches[v.Key] = v
			s.listCachesSize += v.Size()
		}
		return nil
	})

	//set类型
	filepath.Walk(Conf.SetDBPath, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() {
			return nil
		}

		if data, err := ioutil.ReadFile(path); err != nil {
			log.Println(err)
		}else {
			v := decodeSet(data)
			s.setCaches[v.Key] = v
			s.setCachesSize += v.Size()
		}
		return nil
	})

	log.Printf("load db finish, %d Key-cacheValue ",
		len(s.valueCaches)+len(s.mapCaches)+len(s.listCaches)+len(s.setCaches))
}

func (s*Cache) SetOnOP(opFunc func(OpType, ValueCache, ValueCache)) {
	s.opFunction = opFunc
}


/*
value
*/
func (s*Cache) Put(key string, v string, expire int64 ) error{
	s.valueMutex.Lock()
	old, has := s.valueCaches[key]

	if has{
		s.valueCachesSize -= old.Size()
	}

	var needUpdate bool = false
	var val Value
	if expire == ExpireForever {
		val = Value{Key: key, Data: v, Expire:ExpireForever}

		if has {
			if old.Data == val.Data && old.Expire == val.Expire{
				needUpdate = false
			}else{
				needUpdate = true
			}
		}else{
			needUpdate = true
		}
		s.valueCaches[key] = val

		if s.opFunction != nil{
			s.opFunction(Add, &old, &val)
		}
	}else{
		needUpdate = true
		e := time.Now().UnixNano() + expire*int64(time.Second)
		val = Value{Key: key, Data: v, Expire:e}
		s.valueCaches[key] = val
		if s.opFunction != nil{
			s.opFunction(Add, &old, &val)
		}
	}
	s.valueMutex.Unlock()

	log.Printf("put Key:%s, Value:%v, expire:%d", key, v, expire)

	if needUpdate {
		op := persistentValueOp{item: val, opType:Add}
		s.persistentChan <- op
	}

	s.valueCachesSize += val.Size()

	return nil
}

func (s *Cache) Get(key string) (string, error) {
	s.valueMutex.RLock()
	v, ok := s.valueCaches[key]
	s.valueMutex.RUnlock()
	if ok{
		t := time.Now().UnixNano()
		if v.Expire != ExpireForever && v.Expire <= t{
			str := fmt.Sprintf("get Key:%s, not found ", key)
			return "", errors.New(str)
		}else{
			str := fmt.Sprintf("get Key:%s, Value:%s", key, v.Data)
			log.Println(str)
			return v.Data, nil
		}
	}else{
		str := fmt.Sprintf("get Key:%s, not found", key)
		return "", errors.New(str)
	}
}

func (s *Cache) Delete (key string) error{
	s.valueMutex.Lock()
	s.del(key)
	s.valueMutex.Unlock()
	return nil
}

func (s *Cache) ValueCaches() map[string]Value {
	s.valueMutex.Lock()
	defer s.valueMutex.Unlock()

	log.Printf("ValueCaches size:%d", s.valueCachesSize)
	return s.valueCaches
}


func (s *Cache) ClearValue()  {
	s.valueMutex.Lock()
	for k, _:= range s.valueCaches {
		s.del(k)
	}
	s.valueMutex.Unlock()
}

func (s *Cache) del(key string) {

	log.Printf("del Key:%s", key)
	old, ok := s.valueCaches[key]
	if ok{
		s.valueCachesSize -= old.Size()

		delete(s.valueCaches, key)
		val := Value{Key: key, Expire: ExpireForever, Data:""}
		op := persistentValueOp{item: val, opType:Del}
		s.persistentChan <- op

		if s.opFunction != nil{
			s.opFunction(Del, &old, nil)
		}
	}
}

func (s *Cache) saveDataBaseKV(key string, v Value) {
	b := encodeValue(v)

	fullPath := filepath.Join(Conf.ValueDBPath, key)
	path, _ := filepath.Split(fullPath)

	createDir(path)

	err := ioutil.WriteFile(fullPath, b, os.ModePerm)
	if err != nil{
		log.Printf("saveDataBaseKV error:%s", err.Error())
	}
}

func (s *Cache) delDatabaseKV(key string)  {
	fullPath := filepath.Join(Conf.ValueDBPath, key)
	os.Remove(fullPath)
}


/*
map
*/
func (s *Cache) HMPut(hmKey string, keys [] string,  fields [] string, expire int64) error{
	if len(keys) != len(fields){
		return errors.New("map keys len not equal fields len")
	}
	s.mapMutex.Lock()
	defer s.mapMutex.Unlock()

	m, ok := s.mapCaches[hmKey]
	old := MapValue{}
	if !ok{
		m = MapValue{Data: newMapContent(), Key:hmKey, Expire:ExpireForever}
	}else{
		old = MapValue{Data:Copy(m.Data), Key:m.Key, Expire:m.Expire}
		s.mapCachesSize -= old.Size()
	}

	if expire == ExpireForever{
		m.Expire = ExpireForever
	}else{
		m.Expire = time.Now().UnixNano() + expire*int64(time.Second)
	}

	for i:=0; i<len(keys); i++ {
		m.Data[keys[i]] = fields[i]
	}

	s.mapCaches[hmKey] = m

	op := persistentMapOp{item: m, opType: Add}
	s.persistentMapChan <- op

	if s.opFunction != nil{
		s.opFunction(Add, &old, &m)
	}

	s.mapCachesSize += m.Size()

	return nil
}

func (s *Cache) HMGet(hmKey string) (string, error){
	s.mapMutex.RLock()
	defer s.mapMutex.RUnlock()
	v, ok := s.mapCaches[hmKey]
	if !ok {
		str := fmt.Sprintf("not have key:%s map", hmKey)
		return "", errors.New(str)
	}

	t := time.Now().UnixNano()
	if v.Expire != ExpireForever && v.Expire <= t{
		str := fmt.Sprintf("HMGet Key:%s, not found ", hmKey)
		return "", errors.New(str)
	}else{
		str := fmt.Sprintf("HMGet Key:%s, Value:%s", hmKey, v.Data)
		log.Println(str)
		d, err := json.MarshalIndent(v.Data, "", "")
		return string(d), err
	}

}


func (s *Cache) HMGetMember(hmKey string, fieldKey string) (string, error){
	s.mapMutex.RLock()
	defer s.mapMutex.RUnlock()
	v, ok := s.mapCaches[hmKey]
	if !ok {
		str := fmt.Sprintf("not have key:%s map", hmKey)
		return "", errors.New(str)
	}

	t := time.Now().UnixNano()
	if v.Expire != ExpireForever && v.Expire <= t{
		str := fmt.Sprintf("HMGetMember Key:%s, not found ", hmKey)
		return "", errors.New(str)
	}


	d, ok := v.Data[fieldKey]
	if ok {
		return d, nil
	}else{
		str := fmt.Sprintf("%s map not have field: %s", hmKey, fieldKey)
		return "", errors.New(str)
	}
}

func (s *Cache) HMDelMember(hmKey string, fieldKey string) error{
	s.mapMutex.Lock()
	defer s.mapMutex.Unlock()

	m, ok := s.mapCaches[hmKey]
	old := MapValue{}
	if !ok {
		str := fmt.Sprintf("not have key:%s map", hmKey)
		return errors.New(str)
	}
	old = MapValue{Data:Copy(m.Data), Key:m.Key, Expire:m.Expire}

	_, ok1 := m.Data[fieldKey]
	if ok1 {
		s.mapCachesSize -= old.Size()

		delete(m.Data, fieldKey)
		s.mapCaches[hmKey] = m

		op := persistentMapOp{item: m, opType: Del}
		s.persistentMapChan <- op

		if s.opFunction != nil{
			s.opFunction(Del, &old, &m)
		}

		s.mapCachesSize += m.Size()
	}

	return nil
}


func (s *Cache) HMDel(hmKey string) error{
	s.mapMutex.Lock()
	defer s.mapMutex.Unlock()
	s.hDel(hmKey)

	return nil
}

func (s *Cache) ClearMap()  {
	s.mapMutex.Lock()
	for k, _:= range s.mapCaches {
		s.hDel(k)
	}
	s.mapMutex.Unlock()

}


func (s *Cache) MapCaches() map[string]MapValue {
	s.mapMutex.Lock()
	defer s.mapMutex.Unlock()

	log.Printf("MapCaches size:%d", s.mapCachesSize)
	return s.mapCaches
}

func (s *Cache) hDel(key string) {

	log.Printf("hDel Key:%s", key)
	m, ok := s.mapCaches[key]

	if ok {
		old := MapValue{Data:Copy(m.Data), Key:m.Key, Expire:m.Expire}
		s.mapCachesSize -= old.Size()

		delete(s.mapCaches, key)

		m.Data = newMapContent()
		op := persistentMapOp{item: m, opType: Del}
		s.persistentMapChan <- op

		if s.opFunction != nil{
			s.opFunction(Del, &old, &m)
		}
	}
}

func (s *Cache) hSaveDatabaseKV(key string, v MapValue) {
	b := encodeHM(v)

	fullPath := filepath.Join(Conf.MapDBPath, key)
	path, _ := filepath.Split(fullPath)

	createDir(path)

	err := ioutil.WriteFile(fullPath, b, os.ModePerm)
	if err != nil{
		log.Printf("hSaveDatabaseKV error:%s", err.Error())
	}
}

func (s *Cache) hDelDatabase(key string)  {
	fullPath := filepath.Join(Conf.MapDBPath, key)
	os.Remove(fullPath)
}


/*
list
*/
func (s *Cache) LPut(key string, value []string, expire int64) error{
	s.listMutex.Lock()
	defer s.listMutex.Unlock()
	arr, ok := s.listCaches[key]
	old := arr
	if !ok{
		arr = ListValue{Expire:expire, Key:key}
	}else{
		s.listCachesSize -= old.Size()
	}
	arr.Expire = expire
	arr.Data = append(arr.Data, value...)
	s.listCaches[key] = arr

	op := persistentListOp{item: arr, opType: Add}
	s.persistentListChan <- op

	if s.opFunction != nil{
		s.opFunction(Add, &old, &arr)
	}

	s.listCachesSize += arr.Size()

	return nil
}

func (s *Cache) LDel(key string) error{
	s.listMutex.Lock()
	defer s.listMutex.Unlock()
	s.lDel(key)
	return nil
}

func (s *Cache) LGet(key string) ([]string, error){
	s.listMutex.RLock()
	defer s.listMutex.RUnlock()
	v, ok := s.listCaches[key]
	if !ok {
		str := fmt.Sprintf("not have key:%s list", key)
		return []string{}, errors.New(str)
	}

	t := time.Now().UnixNano()
	if v.Expire != ExpireForever && v.Expire <= t{
		str := fmt.Sprintf("LGet Key:%s, not found ", key)
		return []string{}, errors.New(str)
	}else{
		str := fmt.Sprintf("LGet Key:%s, Value:%s", key, v.Data)
		log.Println(str)
		return v.Data, nil
	}

}

func (s *Cache) LGetRange(key string, beg int32, end int32) ([]string, error){

	if beg > end{
		str := fmt.Sprintf("list: %s begin index > end index ", key)
		return []string{}, errors.New(str)
	}

	s.listMutex.RLock()
	defer s.listMutex.RUnlock()

	v, ok := s.listCaches[key]
	if !ok {
		str := fmt.Sprintf("not have key:%s list", key)
		return []string{}, errors.New(str)
	}

	t := time.Now().UnixNano()
	if v.Expire != ExpireForever && v.Expire <= t{
		str := fmt.Sprintf("LGetRange Key:%s, not found ", key)
		return []string{}, errors.New(str)
	}

	l :=len(v.Data)
	if beg >= int32(l){
		str := fmt.Sprintf("list: %s out off range ", key)
		return []string{}, errors.New(str)
	}

	min := int(math.Min(float64(end), float64(l)))
	arr := v.Data[beg:min]

	return arr, nil
}

func (s *Cache) LDelRange(key string, beg int32, end int32)  error{

	if beg > end{
		str := fmt.Sprintf("list: %s begin index > end index ", key)
		return errors.New(str)
	}

	s.listMutex.RLock()
	defer s.listMutex.RUnlock()

	m, ok := s.listCaches[key]
	old := m
	if !ok {
		str := fmt.Sprintf("not have key:%s list", key)
		return errors.New(str)
	}

	l :=len(m.Data)
	if beg >= int32(l){
		str := fmt.Sprintf("list: %s out off range ", key)
		return errors.New(str)
	}

	s.listCachesSize -= old.Size()

	min := int(math.Min(float64(end), float64(l)))
	b := m.Data[0:beg]
	e := m.Data[min:]

	m.Data = append(b, e...)
	s.listCaches[key] = m

	op := persistentListOp{item: m, opType: Del}
	s.persistentListChan <- op

	if s.opFunction != nil{
		s.opFunction(Del, &old, &m)
	}

	s.listCachesSize += m.Size()
	return  nil
}

func (s *Cache) ClearList()  {
	s.listMutex.Lock()
	for k, _:= range s.listCaches {
		s.lDel(k)
	}
	s.listMutex.Unlock()
}

func (s *Cache) ListCaches() map[string]ListValue {
	s.listMutex.Lock()
	defer s.listMutex.Unlock()

	log.Printf("ListCaches size:%d", s.listCachesSize)
	return s.listCaches
}

func (s *Cache) lDel(key string) {

	log.Printf("lDel Key:%s", key)
	m, ok := s.listCaches[key]
	old := m
	if ok {

		s.listCachesSize -= old.Size()
		delete(s.listCaches, key)

		m.Data = []string{}
		op := persistentListOp{item: m, opType: Del}
		s.persistentListChan <- op

		if s.opFunction != nil{
			s.opFunction(Del, &old, &m)
		}

	}
}

func (s *Cache) lSaveDataBaseKV(key string, v ListValue) {
	b := encodeList(v)

	fullPath := filepath.Join(Conf.ListDBPath, key)
	path, _ := filepath.Split(fullPath)

	createDir(path)

	err := ioutil.WriteFile(fullPath, b, os.ModePerm)
	if err != nil{
		log.Printf("lSaveDataBaseKV error:%s", err.Error())
	}
}

func (s *Cache) lDelDatabaseKV(key string)  {
	fullPath := filepath.Join(Conf.ListDBPath, key)
	os.Remove(fullPath)
}


/*
set
*/
func (s *Cache) SPut(key string, value []string, expire int64) error{
	s.setMutex.Lock()
	defer s.setMutex.Unlock()
	arr, ok := s.setCaches[key]
	old := SetValue{}
	if !ok{
		arr = SetValue{Expire:expire, Key:key, Data:newSetContent()}
	}else{
		old = SetValue{Key:arr.Key, Data:Copy(arr.Data), Expire:arr.Expire}
		s.setCachesSize -= old.Size()
	}

	arr.Expire = expire
	for _, v:=range value{
		arr.Data[v] = v
	}

	s.setCaches[key] = arr

	op := persistentSetOp{item: arr, opType: Add}
	s.persistentSetChan <- op

	if s.opFunction != nil{
		s.opFunction(Add, &old, &arr)
	}
	s.setCachesSize += arr.Size()

	return nil
}

func (s *Cache) SGet(key string) ([]string, error){
	s.setMutex.RLock()
	defer s.setMutex.RUnlock()
	v, ok := s.setCaches[key]
	if !ok {
		str := fmt.Sprintf("not have key:%s set", key)
		return []string{}, errors.New(str)
	}

	t := time.Now().UnixNano()
	if v.Expire != ExpireForever && v.Expire <= t{
		str := fmt.Sprintf("SGet Key:%s, not found ", key)
		return []string{}, errors.New(str)
	}else{
		str := fmt.Sprintf("SGet Key:%s, Value:%s", key, v.Data)
		log.Println(str)
		return v.all(), nil
	}
}

func (s *Cache) SDelMember(key string, value string) error{
	s.setMutex.Lock()
	defer s.setMutex.Unlock()

	m, ok := s.setCaches[key]

	if !ok {
		str := fmt.Sprintf("not have key:%s set", key)
		return errors.New(str)
	}


	ok1 := m.isExist(value)
	if ok1 {

		old := SetValue{Key:m.Key, Data:Copy(m.Data), Expire:m.Expire}
		s.setCachesSize -= old.Size()

		m.del(value)
		s.setCaches[key] = m

		op := persistentSetOp{item: m, opType: Del}
		s.persistentSetChan <- op

		if s.opFunction != nil{
			s.opFunction(Del, &old, &m)
		}

		s.setCachesSize += m.Size()
	}

	return nil
}

func (s *Cache) SDel(key string) error{
	s.setMutex.Lock()
	defer s.setMutex.Unlock()
	s.sDel(key)
	return nil
}

func (s *Cache) ClearSet()  {
	s.setMutex.Lock()
	for k, _:= range s.setCaches {
		s.sDel(k)
	}
	s.setMutex.Unlock()
}

func (s *Cache) SetCaches() map[string]SetValue {
	s.setMutex.Lock()
	defer s.setMutex.Unlock()

	log.Printf("SetCaches size:%d", s.setCachesSize)
	return s.setCaches
}

func (s *Cache) sDel(key string) error{
	log.Printf("sDel Key:%s", key)
	m, ok := s.setCaches[key]
	if ok {
		old := SetValue{Key:m.Key, Data:Copy(m.Data), Expire:m.Expire}
		s.setCachesSize -= old.Size()

		delete(s.setCaches, key)
		m.Data = newSetContent()

		op := persistentSetOp{item: m, opType: Del}
		s.persistentSetChan <- op

		if s.opFunction != nil{
			s.opFunction(Del, &old, &m)
		}

	}
	return nil
}

func (s *Cache) sSaveDatabaseKV(key string, v SetValue) {
	b := encodeSet(v)

	fullPath := filepath.Join(Conf.SetDBPath, key)
	path, _ := filepath.Split(fullPath)

	createDir(path)

	err := ioutil.WriteFile(fullPath, b, os.ModePerm)
	if err != nil{
		log.Printf("sSaveDatabaseKV error:%s", err.Error())
	}
}

func (s *Cache) sDelDatabase(key string)  {
	fullPath := filepath.Join(Conf.SetDBPath, key)
	os.Remove(fullPath)
}




func (s *Cache) checkExpire() {
	for {
		time.Sleep(time.Duration(Conf.CheckExpireInterval) * time.Second)
		s.valueMutex.Lock()
		t := time.Now().UnixNano()
		for k, v := range s.valueCaches {
			if v.Expire != ExpireForever && v.Expire <= t{
				s.del(k)
			}
		}
		s.valueMutex.Unlock()

		time.Sleep(time.Second)

		s.mapMutex.Lock()
		t1 := time.Now().UnixNano()
		for k, v := range s.mapCaches  {
			if v.Expire != ExpireForever && v.Expire <= t1{
				s.hDel(k)
			}
		}
		s.mapMutex.Unlock()

		time.Sleep(time.Second)

		s.listMutex.Lock()
		t2 := time.Now().UnixNano()
		for k, v := range s.listCaches  {
			if v.Expire != ExpireForever && v.Expire <= t2{
				s.lDel(k)
			}
		}
		s.listMutex.Unlock()

		time.Sleep(time.Second)

		s.setMutex.Lock()
		t3 := time.Now().UnixNano()
		for k, v := range s.setCaches  {
			if v.Expire != ExpireForever && v.Expire <= t3{
				s.sDel(k)
			}
		}
		s.setMutex.Unlock()
	}
}

func (s *Cache) persistent()  {
	for{
		select {
		case op := <-s.persistentChan:
			v := op.item
			if op.opType == Add {
				s.saveDataBaseKV(v.Key, v)
			}else if op.opType == Del{
				s.delDatabaseKV(v.Key)
			}
		case op := <-s.persistentMapChan:
			v := op.item
			if op.opType == Add {
				s.hSaveDatabaseKV(v.Key, v)
			}else if op.opType == Del{
				if len(v.Data) == 0 {
					s.hDelDatabase(v.Key)
				}else{
					s.hSaveDatabaseKV(v.Key, v)
				}
			}
		case op := <-s.persistentListChan:
			v := op.item
			if op.opType == Add {
				s.lSaveDataBaseKV(v.Key, v)
			}else if op.opType == Del{
				if len(v.Data) == 0 {
					s.lDelDatabaseKV(v.Key)
				}else{
					s.lSaveDataBaseKV(v.Key, v)
				}
			}
		case op := <-s.persistentSetChan:
			v := op.item
			if op.opType == Add {
				s.sSaveDatabaseKV(v.Key, v)
			}else if op.opType == Del{
				if len(v.Data) == 0 {
					s.sDelDatabase(v.Key)
				}else{
					s.sSaveDatabaseKV(v.Key, v)
				}
			}
		}
	}
}

