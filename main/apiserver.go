package main

import (
	"encoding/json"
	"fmt"
	"lightkv/cache"
	"net/http"
	"strconv"
	"strings"
)

const Get = "/get/"
const Push = "/put"
const Delete = "/del/"
const Dump = "/dump"

type httpServer struct {
	cache* cache.Cache
}


type Rsp struct {
	Success bool 		`json:"success"`
	Key     string 		`json:"key"`
	Value   interface{}	`json:"value"`
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := r.URL.Query()
	pathLower := strings.ToLower(r.URL.Path)
	if strings.HasPrefix(pathLower, Get) {

		parts := strings.Split(r.URL.Path[len(Get):], "/")
		if len(parts) != 1{
			r := Rsp{Key: "", Value:nil, Success:false}
			data, _ := json.Marshal(r)
			http.Error(w, string(data), http.StatusBadRequest)
		}else{
			v, ok := s.cache.Get(parts[0])
			if ok {
				r := Rsp{Key: parts[0], Value:v, Success:true}
				data, _ := json.Marshal(r)
				w.Write(data)
			}else{
				r := Rsp{Key: parts[0], Value:"", Success:true}
				data, _ := json.Marshal(r)
				w.Write(data)
			}
		}

	}else if strings.HasPrefix(pathLower, Push){
		//fmt.Printf("key:%s\n",  vars["key"])
		//fmt.Printf("value:%s\n",  vars["value"])

		key, ok1 := vars["key"]
		value, ok2 := vars["value"]
		expire, ok3 := vars["expire"]

		if ok1 == false {
			r := Rsp{Key: "", Value:"", Success:false}
			data, _ := json.Marshal(r)
			http.Error(w, string(data), http.StatusBadRequest)
			return
		}

		if ok2 == false {
			r := Rsp{Key: key[0], Value:"", Success:false}
			data, _ := json.Marshal(r)
			http.Error(w, string(data), http.StatusBadRequest)
			return
		}

		if ok3{
			int64, err := strconv.ParseInt(expire[0], 10, 64)
			if err == nil{
				s.cache.Put(key[0], value[0], int64)
			}else{
				s.cache.Put(key[0], value[0], cache.ExpireForever)
			}
		}else{
			s.cache.Put(key[0], value[0], cache.ExpireForever)
		}
		r := Rsp{Key: key[0], Value:value[0], Success:true}
		data, _ := json.Marshal(r)
		w.Write(data)

	}else if strings.HasPrefix(pathLower, Delete){
		parts := strings.Split(r.URL.Path[len(Delete):], "/")
		if len(parts) != 1{
			r := Rsp{Key: parts[0], Value:"", Success:false}
			data, _ := json.Marshal(r)
			http.Error(w, string(data), http.StatusBadRequest)
		}else{
			s.cache.Delete(parts[0])
			r := Rsp{Key: parts[0], Value:"", Success:true}
			data, _ := json.Marshal(r)
			w.Write(data)
		}
	}else if strings.HasPrefix(pathLower, Dump){

		m := s.cache.All()
		data, _ := json.Marshal(m)
		w.Write(data)

	}else{
		r := Rsp{Key: "", Value:"", Success:false}
		data, _ := json.Marshal(r)
		http.Error(w, string(data), http.StatusBadRequest)
	}
}

func main() {
	h := httpServer{cache:cache.NewCache(15)}
	fmt.Println(http.ListenAndServe(":9981", &h))
}