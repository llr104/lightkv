package cache

type  value struct{
	Key    string       `json:"Key"`
	Expire int64		`json:"expire"`
	Data   string		`json:"data"`
}


