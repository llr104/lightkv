package cache

type  cacheValue struct{
	Expire int64		`json:"expire"`
	Data   []byte		`json:"data"`
}

