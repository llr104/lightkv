package kv

import (
	"encoding/json"
	"time"
	"unsafe"
)

type MapContent map[string] string

func NewMapContent() MapContent{
	return make(MapContent)
}

type MapValue struct {
	Key    string       		`json:"key"`
	Expire int64				`json:"expire"`
	Data   MapContent			`json:"data"`
}

func (s MapValue) ToString() string{
	data, _ := json.MarshalIndent(s.Data, "", "    ")
	return string(data)
}

func (s MapValue) Size() int {
	l := int(unsafe.Sizeof(s.Expire))
	t := l
	for k, v := range s.Data {
		t += len(k)
		t += len(v)
	}
	return t + len(s.Key)
}

func (s MapValue) GetKey() string{
	return s.Key
}

func (s MapValue) IsExpire() bool{
	t := time.Now().UnixNano()
	if s.Expire != ExpireForever && s.Expire <= t{
		return true
	}
	return false
}

func (s MapValue) Add(keys [] string,  fields [] string) {
	for i:=0; i<len(keys); i++ {
		s.Data[keys[i]] = fields[i]
	}
}

func (s MapValue) Get(key string) (string, bool) {
	v, ok := s.Data[key]
	return v, ok
}

func (s MapValue) Remove(key string) {
	delete(s.Data, key)
}

