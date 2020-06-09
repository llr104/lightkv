package kv

import (
	"encoding/json"
	"time"
	"unsafe"
)

type SetContent map[string] string

func NewSetContent() SetContent{
	return make(SetContent)
}


type SetValue struct {
	Key    	string       	`json:"key"`
	Expire 	int64			`json:"expire"`
	Data 	SetContent		`json:"data"`
}

func (s SetValue) Add(v string){
	if s.Data == nil{
		s.Data = make(map[string]string)
	}
	s.Data[v] = v
}

func (s SetValue) Del(v string){
	if s.Data != nil{
		delete(s.Data, v)
	}
}

func (s SetValue) IsExist(v string) bool{
	if s.Data == nil {
		return false
	}
	_, ok := s.Data[v]
	return ok
}

func (s SetValue) all() []string{
	if s.Data == nil{
		s.Data = make(map[string]string)
	}

	l := len(s.Data)
	arr := make([]string, l)
	i := 0
	for k,_ := range s.Data {
		arr[i] = k
		i++
	}
	return arr
}


func (s SetValue) ToString() string{
	data, _ := json.MarshalIndent(s.all(), "", "    ")
	return string(data)
}

func (s SetValue) Size() int {
	l := int(unsafe.Sizeof(s.Expire))
	t := l
	for k, v := range s.Data {
		t += len(k)
		t += len(v)
	}
	return t + len(s.Key)
}

func (s SetValue) GetKey() string{
	return s.Key
}

func (s SetValue) IsExpire() bool{
	t := time.Now().UnixNano()
	if s.Expire != ExpireForever && s.Expire <= t{
		return true
	}
	return false
}

