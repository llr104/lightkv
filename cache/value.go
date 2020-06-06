package cache

import "encoding/json"

type DataString interface {
	ToString() string
}

const (
	ValueData int32 = 0
	MapData   int32 = 1
	ListData  int32 = 2
)

type Value struct{
	Key    string       		`json:"Key"`
	Expire int64				`json:"expire"`
	Data   string				`json:"data"`
}

func (s*Value) ToString() string{
	return s.Data
}

type MapValue struct {
	Key    string       		`json:"Key"`
	Expire int64				`json:"expire"`
	Data   map[string] string	`json:"data"`
}

func (s*MapValue) ToString() string{
	data, _ := json.MarshalIndent(s.Data, "", "    ")
	return string(data)
}

type ListValue struct {
	Key    string       		`json:"Key"`
	Expire int64				`json:"expire"`
	Data   []string				`json:"data"`
}

func (s*ListValue) ToString() string{
	data, _ := json.MarshalIndent(s.Data, "", "    ")
	return string(data)
}
