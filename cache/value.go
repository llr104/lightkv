package cache

import (
	"encoding/json"
	"unsafe"
)


type OpType int32

const (
	Add = 0
	Del = 1
)

type persistentValueOp struct {
	item   Value
	opType OpType
}

type persistentMapOp struct {
	item   MapValue
	opType OpType
}

type persistentListOp struct {
	item   ListValue
	opType OpType
}

type persistentSetOp struct {
	item   SetValue
	opType OpType
}


type ValueCache interface {
	ToString() string
	Size() int
}

const (
	ValueData int32 = 0
	MapData   int32 = 1
	ListData  int32 = 2
	SetData   int32 = 3
)

type Value struct{
	Key    string       		`json:"key"`
	Expire int64				`json:"expire"`
	Data   string				`json:"data"`
}

func (s Value) ToString() string{
	return s.Data
}

func (s Value) Size() int {
	l := int(unsafe.Sizeof(s.Expire))
	return l + len(s.Key) + len(s.Data)
}

type MapContent map[string] string

func newMapContent() MapContent{
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

type SetContent map[string] string

func newSetContent() SetContent{
	return make(SetContent)
}


type SetValue struct {
	Key    	string       	`json:"key"`
	Expire 	int64			`json:"expire"`
	Data 	SetContent		`json:"data"`
}

func (s SetValue) add(v string){
	if s.Data == nil{
		s.Data = make(map[string]string)
	}
	s.Data[v] = v
}

func (s SetValue) del(v string){
	if s.Data != nil{
		delete(s.Data, v)
	}
}

func (s SetValue) isExist(v string) bool{
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

func Copy(m map[string]string) map[string]string{
	r := make(map[string]string)
	for k, v := range m {
		r[k] = v
	}
	return r
}
