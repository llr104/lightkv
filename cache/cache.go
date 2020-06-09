package cache

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/llr104/lightkv/cache/kv"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"time"
)



type Cache struct{
	stringLRU   *lru
	mapLRU		*lru
	listLRU		*lru
	setLRU      *lru

	persistentStringChan chan kv.PersistentStringOp
	persistentMapChan    chan kv.PersistentMapOp
	persistentListChan   chan kv.PersistentListOp
	persistentSetChan    chan kv.PersistentSetOp
	opFunction           func(kv.OpType, kv.ValueCache, kv.ValueCache)

}


func NewCache() *Cache {
	 c := Cache{
	 	stringLRU:			 newLRU(kv.ValueData, Conf.CacheStringSize),
	 	mapLRU:				 newLRU(kv.MapData, Conf.CacheMapSize),
	 	listLRU:			 newLRU(kv.ListData, Conf.CacheListSize),
	 	setLRU:				 newLRU(kv.SetData, Conf.CacheSetSize),

	 	persistentStringChan: make(chan kv.PersistentStringOp),
	 	persistentMapChan:    make(chan kv.PersistentMapOp),
	 	persistentListChan:   make(chan kv.PersistentListOp),
	 	persistentSetChan:    make(chan kv.PersistentSetOp),
	 	opFunction:           nil,
	 }
	 c.init()
	 return &c
}

func (s*Cache) init() {

	s.stringLRU.SetExpireTrigger(s.stringExpire)
	s.mapLRU.SetExpireTrigger(s.mapExpire)
	s.listLRU.SetExpireTrigger(s.listExpire)
	s.setLRU.SetExpireTrigger(s.setExpire)


	createDir(Conf.ValueDBPath)
	createDir(Conf.MapDBPath)
	createDir(Conf.ListDBPath)
	createDir(Conf.SetDBPath)

	s.loadDB()

	go s.persistent()

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
		 	 s.stringLRU.PushFront(v)
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
			s.mapLRU.PushFront(v)
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
			s.listLRU.PushFront(v)
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
			s.setLRU.PushFront(v)
		}
		return nil
	})

	 size := s.stringLRU.Size()+s.mapLRU.Size()+s.listLRU.Size()+s.setLRU.Size()
	 len := s.stringLRU.Len()+s.mapLRU.Len()+s.listLRU.Len()+s.setLRU.Len()
	 log.Printf("load db finish, %d Key-cacheValue memory: %.2f kb", len, float32(size)/1024.0)
}

func (s*Cache) SetOnOP(opFunc func(kv.OpType, kv.ValueCache, kv.ValueCache)) {
	s.opFunction = opFunc
}


/*
StringValue
*/
func (s*Cache) Put(key string, v string, expire int64) error{

	var newVal kv.ValueCache = nil
	oldVal, _ := s.stringLRU.Value(key)

	if oldVal == nil{
		oldVal = kv.StringValue{Key: key}
	}

	if expire == kv.ExpireForever {
		newVal = kv.StringValue{Key: key, Data: v, Expire:kv.ExpireForever}
	}else{
		e := time.Now().UnixNano() + expire*int64(time.Second)
		newVal = kv.StringValue{Key: key, Data: v, Expire:e}
	}
	s.stringLRU.PushFront(newVal)

	if s.opFunction != nil{
		s.opFunction(kv.Add, oldVal, newVal)
	}

	t := newVal.(kv.StringValue)

	op := kv.PersistentStringOp{Item: t, OpType: kv.Add}
	s.persistentStringChan <- op

	return nil
}

func (s *Cache) Get(key string) (string, error) {

	if v, err := s.stringLRU.Value(key); err == nil{
		if v.IsExpire() {
			str := fmt.Sprintf("Get Key:%s, is expire ", key)
			return "", errors.New(str)
		}else{
			return v.ToString(), nil
		}
	}else{
		str := fmt.Sprintf("Get Key:%s, not found", key)
		return "", errors.New(str)
	}

}

func (s *Cache) Delete (key string) error{
	return s.del(key)
}

func (s *Cache) StringCaches() ([]byte, error){
	return s.stringLRU.CacheToString()
}

func (s *Cache) ClearString()  {
	s.stringLRU.Clear()
	op := kv.PersistentStringOp{OpType: kv.Clear}
	s.persistentStringChan <- op
}

func (s *Cache) del(key string) error{
	oldVal, err := s.stringLRU.Value(key)
	if err != nil{
		return err
	}

	log.Printf("del Key:%s", key)
	s.stringLRU.Remove(key)

	s.stringExpire(key, oldVal)

	return nil
}

func (s *Cache) stringExpire(key string, v kv.ValueCache){

	val := kv.StringValue{Key: key, Expire: kv.ExpireForever, Data:""}
	op := kv.PersistentStringOp{Item: val, OpType: kv.Del}
	s.persistentStringChan <- op

	if s.opFunction != nil{
		s.opFunction(kv.Del, v, nil)
	}
}

func (s *Cache) saveString(key string, v kv.StringValue) {
	b := encodeValue(v)

	fullPath := filepath.Join(Conf.ValueDBPath, key)
	path, _ := filepath.Split(fullPath)

	createDir(path)

	err := ioutil.WriteFile(fullPath, b, os.ModePerm)
	if err != nil{
		log.Printf("saveString error:%s", err.Error())
	}
}

func (s *Cache) delString(key string)  {
	fullPath := filepath.Join(Conf.ValueDBPath, key)
	os.Remove(fullPath)
}

func (s *Cache) clearString()  {
	os.RemoveAll(Conf.ValueDBPath)
	createDir(Conf.ValueDBPath)
}

/*
map
*/
func (s *Cache) HMPut(hmKey string, keys [] string,  fields [] string, expire int64) error{
	if len(keys) != len(fields){
		return errors.New("map keys len not equal fields len")
	}

	val, err := s.mapLRU.Value(hmKey)
	m := kv.MapValue{}
	old := kv.MapValue{Key:hmKey}
	if err != nil{
		m = kv.MapValue{Data: kv.NewMapContent(), Key:hmKey, Expire:kv.ExpireForever}
	}else{
		old = kv.MapValue{Data: kv.Copy(m.Data), Key:m.Key, Expire:m.Expire}
		m = val.(kv.MapValue)
	}

	if expire == kv.ExpireForever{
		m.Expire = kv.ExpireForever
	}else{
		m.Expire = time.Now().UnixNano() + expire*int64(time.Second)
	}

	m.Add(keys, fields)
	s.mapLRU.PushFront(m)

	op := kv.PersistentMapOp{Item: m, OpType: kv.Add}
	s.persistentMapChan <- op

	if s.opFunction != nil{
		s.opFunction(kv.Add, old, m)
	}

	return nil
}

func (s *Cache) HMGet(hmKey string) (string, error){
	if v, err := s.mapLRU.Value(hmKey); err == nil{
		if v.IsExpire() {
			str := fmt.Sprintf("HMGet Key:%s, is expire ", hmKey)
			return "", errors.New(str)
		}else{
			return v.ToString(), nil
		}
	}else{
		str := fmt.Sprintf("HMGet Key:%s, not found", hmKey)
		return "", errors.New(str)
	}
}


func (s *Cache) HMGetMember(hmKey string, fieldKey string) (string, error){

	val, err := s.mapLRU.Value(hmKey)

	if err != nil {
		str := fmt.Sprintf("HMGetMember not have key:%s map", hmKey)
		return "", errors.New(str)
	}

	if val.IsExpire(){
		str := fmt.Sprintf("HMGetMember Key:%s, not found ", hmKey)
		return "", errors.New(str)
	}

	v := val.(kv.MapValue)
	d, ok := v.Data[fieldKey]
	if ok {
		return d, nil
	}else{
		str := fmt.Sprintf("HMGetMember key: %s map not have field: %s", hmKey, fieldKey)
		return "", errors.New(str)
	}
}

func (s *Cache) HMDelMember(hmKey string, fieldKey string) error{

	val, err := s.mapLRU.Value(hmKey)

	if err != nil {
		str := fmt.Sprintf("HMDelMember not have key:%s map", hmKey)
		return errors.New(str)
	}

	if val.IsExpire(){
		str := fmt.Sprintf("HMDelMember Key:%s, not found ", hmKey)
		return errors.New(str)
	}


	m := val.(kv.MapValue)
	old := kv.MapValue{Data: kv.Copy(m.Data), Key:m.Key, Expire:m.Expire}

	_, ok1 := m.Get(fieldKey)
	if ok1 {
		m.Remove(fieldKey)
		op := kv.PersistentMapOp{Item: m, OpType: kv.Del}
		s.persistentMapChan <- op

		if s.opFunction != nil{
			s.opFunction(kv.Del, old, m)
		}
	}

	return nil
}


func (s *Cache) HMDel(hmKey string) error{
	return s.hDel(hmKey)
}

func (s *Cache) ClearMap()  {
	s.mapLRU.Clear()
	op := kv.PersistentMapOp{OpType: kv.Clear}
	s.persistentMapChan <- op
}


func (s *Cache) MapCaches() ([]byte, error) {
	return s.mapLRU.CacheToString()
}

func (s *Cache) hDel(key string) error{

	oldVal, err := s.mapLRU.Value(key)
	if err != nil{
		return err
	}

	log.Printf("hDel Key:%s", key)
	s.mapLRU.Remove(key)

	s.mapExpire(key, oldVal)

	return nil
}

func (s *Cache) mapExpire(key string, v kv.ValueCache){

	val := kv.MapValue{Key: key, Expire: kv.ExpireForever, Data: kv.NewMapContent()}
	op := kv.PersistentMapOp{Item: val, OpType: kv.Del}
	s.persistentMapChan <- op

	if s.opFunction != nil{
		s.opFunction(kv.Del, v, nil)
	}
}

func (s *Cache) saveMap(key string, v kv.MapValue) {
	b := encodeHM(v)

	fullPath := filepath.Join(Conf.MapDBPath, key)
	path, _ := filepath.Split(fullPath)

	createDir(path)

	err := ioutil.WriteFile(fullPath, b, os.ModePerm)
	if err != nil{
		log.Printf("saveMap error:%s", err.Error())
	}
}

func (s *Cache) delMap(key string)  {
	fullPath := filepath.Join(Conf.MapDBPath, key)
	os.Remove(fullPath)
}

func (s *Cache) clearMap()  {
	os.RemoveAll(Conf.MapDBPath)
	createDir(Conf.MapDBPath)
}


/*
list
*/
func (s *Cache) LPut(key string, value []string, expire int64) error{

	var newVal kv.ValueCache = nil
	var oldVal kv.ValueCache = nil

	if v, err := s.listLRU.Value(key); err != nil {
		n := kv.ListValue{Expire: expire, Key:key, Data:[]string{}}
		n.Expire = expire
		n.Data = append(n.Data, value...)
		newVal = n
		oldVal = kv.ListValue{Key:key}
	}else{
		oldVal = v
		n := v.(kv.ListValue)
		n.Expire = expire
		n.Data = append(n.Data, value...)
		newVal = n
	}

	s.listLRU.PushFront(newVal)
	op := kv.PersistentListOp{Item: newVal.(kv.ListValue), OpType: kv.Add}
	s.persistentListChan <- op

	if s.opFunction != nil{
		s.opFunction(kv.Add, oldVal, newVal)
	}

	return nil
}

func (s *Cache) LDel(key string) error{
	return s.lDel(key)
}

func (s *Cache) LGet(key string) ([]string, error){

	if v, err := s.listLRU.Value(key); err == nil{
		if v.IsExpire() {
			str := fmt.Sprintf("LGet Key:%s, is expire ", key)
			return []string{}, errors.New(str)
		}else{
			var arr []string
			json.Unmarshal([]byte(v.ToString()), &arr)
			return arr, nil
		}
	}else{
		str := fmt.Sprintf("LGet Key:%s, not found", key)
		return []string{}, errors.New(str)
	}

}

func (s *Cache) LGetRange(key string, beg int32, end int32) ([]string, error){

	if beg > end{
		str := fmt.Sprintf("list: %s begin index > end index ", key)
		return []string{}, errors.New(str)
	}

	if v, err := s.listLRU.Value(key); err == nil{
		if v.IsExpire() {
			str := fmt.Sprintf("LGetRange Key:%s, is expire ", key)
			return []string{}, errors.New(str)
		}else{
			var arr []string
			json.Unmarshal([]byte(v.ToString()), &arr)

			l := len(arr)
			min := int(math.Min(float64(end), float64(l)))
			r := arr[beg:min]
			return r, nil
		}
	}else{
		str := fmt.Sprintf("LGetRange Key:%s, not found", key)
		return []string{}, errors.New(str)
	}

}

func (s *Cache) LDelRange(key string, beg int32, end int32)  error{

	if beg > end{
		str := fmt.Sprintf("list: %s begin index > end index ", key)
		return errors.New(str)
	}

	val, err := s.listLRU.Value(key)
	if err != nil{
		str := fmt.Sprintf("not have key:%s list", key)
		return errors.New(str)
	}
	m := val.(kv.ListValue)
	oldVar := kv.ListValue{Key: m.Key, Data:m.Data, Expire:m.Expire}

	l :=len(m.Data)
	if beg >= int32(l){
		str := fmt.Sprintf("list: %s out off range ", key)
		return errors.New(str)
	}

	min := int(math.Min(float64(end), float64(l)))
	b := m.Data[0:beg]
	e := m.Data[min:]

	m.Data = append(b, e...)

	s.listLRU.PushFront(m)

	op := kv.PersistentListOp{Item: m, OpType: kv.Del}
	s.persistentListChan <- op

	if s.opFunction != nil{
		s.opFunction(kv.Del, oldVar, m)
	}

	return  nil
}

func (s *Cache) ClearList()  {
	s.listLRU.Clear()
	op := kv.PersistentListOp{OpType: kv.Clear}
	s.persistentListChan <- op
}

func (s *Cache) ListCaches() ([]byte, error) {
	return s.listLRU.CacheToString()
}


func (s *Cache) lDel(key string) error{

	oldVal, err := s.listLRU.Value(key)
	if err != nil{
		return err
	}
	log.Printf("lDel Key:%s", key)

	s.listLRU.Remove(key)
	s.listExpire(key, oldVal)
	return nil
}

func (s *Cache) listExpire(key string, v kv.ValueCache){

	val := kv.StringValue{Key: key, Expire: kv.ExpireForever, Data:""}
	op := kv.PersistentStringOp{Item: val, OpType: kv.Del}
	s.persistentStringChan <- op

	if s.opFunction != nil{
		s.opFunction(kv.Del, v, nil)
	}
}

func (s *Cache) saveList(key string, v kv.ListValue) {
	b := encodeList(v)

	fullPath := filepath.Join(Conf.ListDBPath, key)
	path, _ := filepath.Split(fullPath)

	createDir(path)

	err := ioutil.WriteFile(fullPath, b, os.ModePerm)
	if err != nil{
		log.Printf("saveList error:%s", err.Error())
	}
}

func (s *Cache) delList(key string)  {
	fullPath := filepath.Join(Conf.ListDBPath, key)
	os.Remove(fullPath)
}


func (s *Cache) clearList()  {
	os.RemoveAll(Conf.ListDBPath)
	createDir(Conf.ListDBPath)
}


/*
set
*/
func (s *Cache) SPut(key string, value []string, expire int64) error{

	oldVal, err := s.setLRU.Value(key)
	var newVal kv.ValueCache
	if err != nil{
		sv := kv.SetValue{Expire: expire, Key:key, Data: kv.NewSetContent()}
		for _,v := range value{
			sv.Add(v)
		}
		newVal = sv
		s.setLRU.PushFront(newVal)
		oldVal = kv.SetValue{Key:key}

	}else{
		sv := oldVal.(kv.SetValue)
		sv.Expire = expire
		for _,v := range value{
			sv.Add(v)
		}
		newVal = sv
		s.setLRU.PushFront(newVal)
	}

	op := kv.PersistentSetOp{Item: newVal.(kv.SetValue), OpType: kv.Add}
	s.persistentSetChan <- op

	if s.opFunction != nil{
		s.opFunction(kv.Add, oldVal, newVal)
	}

	return nil
}

func (s *Cache) SGet(key string) ([]string, error){

	if v, err := s.setLRU.Value(key); err == nil{
		if v.IsExpire() {
			str := fmt.Sprintf("SGet Key:%s, is expire ", key)
			return []string{}, errors.New(str)
		}else{
			var arr []string
			json.Unmarshal([]byte(v.ToString()), &arr)
			return arr, nil
		}
	}else{
		str := fmt.Sprintf("SGet Key:%s, not found", key)
		return []string{}, errors.New(str)
	}

}

func (s *Cache) SDelMember(key string, value string) error{

	oldVal, err := s.setLRU.Value(key)
	if err != nil {
		str := fmt.Sprintf("not have key:%s set", key)
		return errors.New(str)
	}

	m := oldVal.(kv.SetValue)
	old := oldVal.(kv.SetValue)
	ok := m.IsExist(value)
	if ok {

		m.Del(value)
		s.setLRU.PushFront(m)

		op := kv.PersistentSetOp{Item: m, OpType: kv.Del}
		s.persistentSetChan <- op

		if s.opFunction != nil{
			s.opFunction(kv.Del, old, m)
		}
	}

	return nil
}

func (s *Cache) SDel(key string) error{
	return s.sDel(key)
}

func (s *Cache) ClearSet()  {
	s.setLRU.Clear()
	op := kv.PersistentSetOp{OpType: kv.Clear}
	s.persistentSetChan <- op
}

func (s *Cache) SetCaches() ([]byte, error) {
	return s.setLRU.CacheToString()
}

func (s *Cache) sDel(key string) error{
	oldVal, err := s.setLRU.Value(key)
	if err != nil{
		return err
	}

	log.Printf("sDel Key:%s", key)
	s.setLRU.Remove(key)

	s.setExpire(key, oldVal)

	return nil
}

func (s *Cache) setExpire(key string, v kv.ValueCache){

	val := kv.SetValue{Key: key, Expire: kv.ExpireForever, Data: kv.NewSetContent()}
	op := kv.PersistentSetOp{Item: val, OpType: kv.Del}
	s.persistentSetChan <- op

	if s.opFunction != nil{
		s.opFunction(kv.Del, v, nil)
	}
}


func (s *Cache) saveSet(key string, v kv.SetValue) {
	b := encodeSet(v)

	fullPath := filepath.Join(Conf.SetDBPath, key)
	path, _ := filepath.Split(fullPath)

	createDir(path)

	err := ioutil.WriteFile(fullPath, b, os.ModePerm)
	if err != nil{
		log.Printf("saveSet error:%s", err.Error())
	}
}

func (s *Cache) delSet(key string)  {
	fullPath := filepath.Join(Conf.SetDBPath, key)
	os.Remove(fullPath)
}

func (s *Cache) clearSet()  {
	os.RemoveAll(Conf.SetDBPath)
	createDir(Conf.SetDBPath)
}



func (s *Cache) persistent()  {
	for{
		select {
		case op := <-s.persistentStringChan:
			v := op.Item
			if op.OpType == kv.Add {
				s.saveString(v.Key, v)
			}else if op.OpType == kv.Del {
				s.delString(v.Key)
			}else if op.OpType == kv.Clear {
				s.clearString()
			}
		case op := <-s.persistentMapChan:
			v := op.Item
			if op.OpType == kv.Add {
				s.saveMap(v.Key, v)
			}else if op.OpType == kv.Del {
				if len(v.Data) == 0 {
					s.delMap(v.Key)
				}else{
					s.saveMap(v.Key, v)
				}
			}else if op.OpType == kv.Clear {
				s.clearMap()
			}
		case op := <-s.persistentListChan:
			v := op.Item
			if op.OpType == kv.Add {
				s.saveList(v.Key, v)
			}else if op.OpType == kv.Del {
				if len(v.Data) == 0 {
					s.delList(v.Key)
				}else{
					s.saveList(v.Key, v)
				}
			}else if op.OpType == kv.Clear {
				s.clearList()
			}
		case op := <-s.persistentSetChan:
			v := op.Item
			if op.OpType == kv.Add {
				s.saveSet(v.Key, v)
			}else if op.OpType == kv.Del {
				if len(v.Data) == 0 {
					s.delSet(v.Key)
				}else{
					s.saveSet(v.Key, v)
				}
			}else if op.OpType == kv.Clear {
				s.clearSet()
			}
		}
	}
}

