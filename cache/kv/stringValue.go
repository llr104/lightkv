package kv

import (
	"time"
	"unsafe"
)

type StringValue struct{
	Key    string       		`json:"key"`
	Expire int64				`json:"expire"`
	Data   string				`json:"data"`
}

func (s StringValue) ToString() string{
	return s.Data
}

func (s StringValue) Size() int {
	l := int(unsafe.Sizeof(s.Expire))
	return l + len(s.Key) + len(s.Data)
}

func (s StringValue) GetKey() string{
	return s.Key
}

func (s StringValue) IsExpire() bool{
	t := time.Now().UnixNano()
	if s.Expire != ExpireForever && s.Expire <= t{
		return true
	}
	return false
}