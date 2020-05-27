package cache

type  value struct{
	Expire int64		`json:"expire"`
	Data   interface{}	`json:"data"`
}

