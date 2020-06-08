package cache

import "encoding/json"

type DataString interface {
	ToString() string
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

type ListValue struct {
	Key    string       		`json:"key"`
	Expire int64				`json:"expire"`
	Data   []string				`json:"data"`
}

func (s ListValue) ToString() string{
	data, _ := json.MarshalIndent(s.Data, "", "    ")
	return string(data)
}

type SetContent map[string] string

func newSetContent() SetContent{
	return make(SetContent)
}


type SetValue struct {
	Key    	string       	`json:"key"`
	Expire 	int64			`json:"expire"`
	Data 	SetContent			`json:"data"`
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

func Copy(m map[string]string) map[string]string{
	r := make(map[string]string)
	for k, v := range m {
		r[k] = v
	}
	return r
}
