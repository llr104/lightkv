package kv

import (
	"encoding/json"
	"time"
	"unsafe"
)

type ListValue struct {
	Key    string       		`json:"key"`
	Expire int64				`json:"expire"`
	Data   []string				`json:"data"`
}

func (s ListValue) ToString() string{
	data, _ := json.MarshalIndent(s.Data, "", "    ")
	return string(data)
}

func (s ListValue) Size() int {
	l := int(unsafe.Sizeof(s.Expire))
	t := l
	for _, v := range s.Data {
		t += len(v)
	}
	return t + len(s.Key)
}

func (s ListValue) GetKey() string{
	return s.Key
}

func (s ListValue) IsExpire() bool{
	t := time.Now().UnixNano()
	if s.Expire != ExpireForever && s.Expire <= t{
		return true
	}
	return false
}
