package kv


type OpType int32

const (
	Add = 0
	Del = 1
	Clear = 2
)

const ExpireForever = 0

type PersistentStringOp struct {
	Item   StringValue
	OpType OpType
}

type PersistentMapOp struct {
	Item   MapValue
	OpType OpType
}

type PersistentListOp struct {
	Item   ListValue
	OpType OpType
}

type PersistentSetOp struct {
	Item   SetValue
	OpType OpType
}


type ValueCache interface {
	ToString() string
	Size() int
	GetKey() string
	IsExpire() bool
}

const (
	ValueData int32 = 0
	MapData   int32 = 1
	ListData  int32 = 2
	SetData   int32 = 3
)


func Copy(m map[string]string) map[string]string{
	r := make(map[string]string)
	for k, v := range m {
		r[k] = v
	}
	return r
}

