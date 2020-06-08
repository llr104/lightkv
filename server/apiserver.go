package server

import (
	"encoding/json"
	"fmt"
	"github.com/llr104/lightkv/cache"
	"net/http"
	"strconv"
	"strings"
)

const Get = "/get/"
const Push = "/put"
const Delete = "/del/"
const Dump = "/dump"

const HGet = "/hget/"
const HPush = "/hput"
const HGetM = "/hgetm/"
const HDelM = "/hdelm/"
const HDel = "/hdel/"
const HDump = "/hdump"

const LGet = "/lget/"
const LGetRange = "/lgetr/"
const LPush = "/lput"
const LDel = "/ldel/"
const LDelRange = "/ldelr/"
const LDump = "/ldump"

const SGet = "/sget/"
const SPush = "/sput"
const SDel = "/sdel/"
const SDelMember = "/sdelm/"
const SDump = "/sdump"


type apiServer struct {
	cache * cache.Cache
}

type Rsp struct {
	Success bool 		`json:"success"`
	Key     string 		`json:"key"`
	Value   interface{}	`json:"value"`
}

func NewApi(c *cache.Cache) *apiServer {
	h := apiServer{cache: c}
	return &h
}

func (s *apiServer) Start()  {
	fmt.Println(http.ListenAndServe(":9981", s))
}


func (s *apiServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	pathLower := strings.ToLower(r.URL.Path)
	if strings.HasPrefix(pathLower, Get) {
		s.get(w, r)
	}else if strings.HasPrefix(pathLower, Push){
		s.put(w, r)
	}else if strings.HasPrefix(pathLower, Delete){
		s.del(w, r)
	}else if pathLower == Dump{
		s.dump(w, r)
	}else if strings.HasPrefix(pathLower, HGet){
		s.hGet(w, r)
	}else if strings.HasPrefix(pathLower, HGetM){
		s.hGetM(w, r)
	}else if strings.HasPrefix(pathLower, HPush){
		s.hPush(w, r)
	}else if strings.HasPrefix(pathLower, HDel){
		s.hDel(w, r)
	}else if strings.HasPrefix(pathLower, HDelM){
		s.hDelM(w, r)
	}else if pathLower == HDump{
		s.hDump(w, r)
	}else if strings.HasPrefix(pathLower, LGet){
		s.lGet(w, r)
	}else if strings.HasPrefix(pathLower, LGetRange){
		s.lGetRange(w, r)
	}else if strings.HasPrefix(pathLower, LPush){
		s.lPush(w, r)
	}else if strings.HasPrefix(pathLower, LDel){
		s.lDel(w, r)
	}else if strings.HasPrefix(pathLower, LDelRange){
		s.lDelRange(w, r)
	}else if pathLower == LDump{
		s.lDump(w, r)
	}else if strings.HasPrefix(pathLower, SGet) {
		s.sGet(w, r)
	}else if strings.HasPrefix(pathLower, SPush) {
		s.sPush(w, r)
	}else if strings.HasPrefix(pathLower, SDel) {
		s.sDel(w, r)
	}else if strings.HasPrefix(pathLower, SDelMember) {
		s.sDelMember(w, r)
	}else if pathLower == SDump{
		s.sDump(w, r)
	}else{
		r := Rsp{Key: "", Value: "", Success: false}
		data, _ := json.Marshal(r)
		http.Error(w, string(data), http.StatusBadRequest)
	}
}

func (s *apiServer) get(w http.ResponseWriter, r *http.Request){
	parts := strings.Split(r.URL.Path[len(Get):], "/")
	if len(parts) != 1{
		r := Rsp{Key: "", Value: "", Success: false}
		data, _ := json.Marshal(r)
		http.Error(w, string(data), http.StatusBadRequest)
	}else{
		v, err := s.cache.Get(parts[0])
		if err == nil {
			r := Rsp{Key: parts[0], Value:string(v), Success: true}
			data, _ := json.Marshal(r)
			w.Write(data)
		}else{
			r := Rsp{Key: parts[0], Value: "", Success: false}
			data, _ := json.Marshal(r)
			w.Write(data)
		}
	}
}

func (s *apiServer) put(w http.ResponseWriter, r *http.Request){
	//fmt.Printf("key:%s\n",  vars["key"])
	//fmt.Printf("value:%s\n",  vars["value"])
	vars := r.URL.Query()
	key, ok1 := vars["key"]
	value, ok2 := vars["value"]
	expire, ok3 := vars["expire"]

	if ok1 == false {
		r := Rsp{Key: "", Value:"", Success: false}
		data, _ := json.Marshal(r)
		http.Error(w, string(data), http.StatusBadRequest)
		return
	}

	if ok2 == false {
		r := Rsp{Key: key[0], Value:"", Success: false}
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
	rsp := Rsp{Key: key[0], Value:value[0], Success: true}
	data, _ := json.Marshal(rsp)
	w.Write(data)
}

func (s *apiServer) del(w http.ResponseWriter, r *http.Request){
	parts := strings.Split(r.URL.Path[len(Delete):], "/")
	if len(parts) != 1{
		r := Rsp{Key: parts[0], Value:"", Success: false}
		data, _ := json.Marshal(r)
		http.Error(w, string(data), http.StatusBadRequest)
	}else{
		s.cache.Delete(parts[0])
		r := Rsp{Key: parts[0], Value:"", Success: true}
		data, _ := json.Marshal(r)
		w.Write(data)
	}
}

func (s *apiServer) dump(w http.ResponseWriter, r *http.Request){
	data, _ := s.cache.ValueCaches()
	w.Write(data)
}

func (s *apiServer) hGet(w http.ResponseWriter, r *http.Request){
	parts := strings.Split(r.URL.Path[len(HGet):], "/")
	if len(parts) != 1{
		r := Rsp{Key: "", Value: "", Success: false}
		data, _ := json.Marshal(r)
			http.Error(w, string(data), http.StatusBadRequest)
		}else{
			v, err := s.cache.HMGet(parts[0])
			if err == nil {
				r := Rsp{Key: parts[0], Value:v, Success: true}
				data, _ := json.Marshal(r)
				w.Write(data)
			}else{
				r := Rsp{Key: parts[0], Value:"", Success: false}
				data, _ := json.Marshal(r)
				w.Write(data)
			}
	}
}

func (s *apiServer) hGetM(w http.ResponseWriter, r *http.Request){
	parts := strings.Split(r.URL.Path[len(HGetM):], "/")
	if len(parts) != 2{
		r := Rsp{Key: "", Value: "", Success: false}
		data, _ := json.Marshal(r)
		http.Error(w, string(data), http.StatusBadRequest)
	}else{
		v, err := s.cache.HMGetMember(parts[0], parts[1])
		if err == nil {
			r := Rsp{Key: parts[1], Value:v, Success: true}
			data, _ := json.Marshal(r)
			w.Write(data)
		}else{
			r := Rsp{Key: parts[1], Value:"", Success: false}
			data, _ := json.Marshal(r)
			w.Write(data)
		}
	}
}

func (s *apiServer) hPush(w http.ResponseWriter, r *http.Request){
	//fmt.Printf("key:%s\n",  vars["key"])
	//fmt.Printf("value:%s\n",  vars["value"])
	vars := r.URL.Query()
	hmkey, ok0 := vars["hmkey"]
	key, ok1 := vars["key"]
	value, ok2 := vars["value"]
	expire, ok3 := vars["expire"]

	if ok0 == false{
		r := Rsp{Key: "", Value:"", Success: false}
		data, _ := json.Marshal(r)
		http.Error(w, string(data), http.StatusBadRequest)
		return
	}

	if ok1 == false {
		r := Rsp{Key: "", Value:"", Success: false}
		data, _ := json.Marshal(r)
		http.Error(w, string(data), http.StatusBadRequest)
		return
	}

	if ok2 == false {
		r := Rsp{Key: key[0], Value:"", Success: false}
		data, _ := json.Marshal(r)
		http.Error(w, string(data), http.StatusBadRequest)
		return
	}

	if ok3{
		int64, err := strconv.ParseInt(expire[0], 10, 64)
		if err == nil{
			s.cache.HMPut(hmkey[0], key, value, int64)
		}else{
			s.cache.HMPut(hmkey[0], key, value, cache.ExpireForever)
		}
	}else{
		s.cache.HMPut(hmkey[0], key, value, cache.ExpireForever)
	}

	str, _ := s.cache.HMGet(hmkey[0])
	rsp := Rsp{Key: hmkey[0], Value:str, Success: true}
	data, _ := json.Marshal(rsp)
	w.Write(data)
}

func (s *apiServer) hDel(w http.ResponseWriter, r *http.Request){
	parts := strings.Split(r.URL.Path[len(HDel):], "/")
	if len(parts) != 1{
		r := Rsp{Key: parts[0], Value:"", Success: false}
		data, _ := json.Marshal(r)
		http.Error(w, string(data), http.StatusBadRequest)
	}else{
		s.cache.HMDel(parts[0])
		r := Rsp{Key: parts[0], Value:"", Success: true}
		data, _ := json.Marshal(r)
		w.Write(data)
	}
}

func (s *apiServer) hDelM(w http.ResponseWriter, r *http.Request){
	parts := strings.Split(r.URL.Path[len(HDelM):], "/")
	if len(parts) != 2{
		r := Rsp{Key: parts[0], Value:"", Success: false}
		data, _ := json.Marshal(r)
		http.Error(w, string(data), http.StatusBadRequest)
	}else{
		s.cache.HMDelMember(parts[0], parts[1])
		r := Rsp{Key: parts[0], Value:"", Success: true}
		data, _ := json.Marshal(r)
		w.Write(data)
	}
}

func (s *apiServer) hDump(w http.ResponseWriter, r *http.Request){
	data, _ := s.cache.MapCaches()
	w.Write(data)
}

func (s *apiServer) lGet(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path[len(LGet):], "/")
	if len(parts) != 1{
		r := Rsp{Key: "", Value: "", Success: false}
		data, _ := json.Marshal(r)
		http.Error(w, string(data), http.StatusBadRequest)
	}else{
		v, err := s.cache.LGet(parts[0])
		if err == nil {
			r := Rsp{Key: parts[0], Value:v, Success: true}
			data, _ := json.Marshal(r)
			w.Write(data)
		}else{
			r := Rsp{Key: parts[0], Value:"", Success: false}
			data, _ := json.Marshal(r)
			w.Write(data)
		}
	}
}

func (s *apiServer) lGetRange(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path[len(LGetRange):], "/")
	if len(parts) != 1{
		r := Rsp{Key: "", Value: "", Success: false}
		data, _ := json.Marshal(r)
		http.Error(w, string(data), http.StatusBadRequest)
	}else{
		vars := r.URL.Query()
		beg, ok1 := vars["begIndex"]
		end, ok2 := vars["endIndex"]
		if ok1 == false || ok2 == false{
			r := Rsp{Key: "", Value: "", Success: false}
			data, _ := json.Marshal(r)
			http.Error(w, string(data), http.StatusBadRequest)
		}else{
			begInt, err1 := strconv.Atoi(beg[0])
			endInt, err2 := strconv.Atoi(end[0])

			if err1 != nil || err2 != nil{
				r := Rsp{Key: "", Value: "", Success: false}
				data, _ := json.Marshal(r)
				http.Error(w, string(data), http.StatusBadRequest)
			}else{
				v, err := s.cache.LGetRange(parts[0], int32(begInt), int32(endInt))
				if err == nil {
					r := Rsp{Key: parts[0], Value:v, Success: true}
					data, _ := json.Marshal(r)
					w.Write(data)
				}else{
					r := Rsp{Key: parts[0], Value: "", Success: false}
					data, _ := json.Marshal(r)
					w.Write(data)
				}
			}
		}
	}
}

func (s *apiServer) lPush(w http.ResponseWriter, r *http.Request) {
	vars := r.URL.Query()
	key, ok1 := vars["key"]
	value, ok2 := vars["value"]
	expire, ok3 := vars["expire"]

	if ok1 == false {
		r := Rsp{Key: "", Value: "", Success: false}
		data, _ := json.Marshal(r)
		http.Error(w, string(data), http.StatusBadRequest)
		return
	}

	if ok2 == false {
		r := Rsp{Key: key[0], Value: "", Success: false}
		data, _ := json.Marshal(r)
		http.Error(w, string(data), http.StatusBadRequest)
		return
	}

	if ok3{
		int64, err := strconv.ParseInt(expire[0], 10, 64)
		if err == nil{
			s.cache.LPut(key[0], value, int64)
		}else{
			s.cache.LPut(key[0], value, cache.ExpireForever)
		}
	}else{
		s.cache.LPut(key[0], value, cache.ExpireForever)
	}

	rsp := Rsp{Key: key[0], Value:value, Success: true}
	data, _ := json.Marshal(rsp)
	w.Write(data)
}

func (s *apiServer) lDel(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path[len(LDel):], "/")
	if len(parts) != 1{
		r := Rsp{Key: parts[0], Value: "", Success: false}
		data, _ := json.Marshal(r)
		http.Error(w, string(data), http.StatusBadRequest)
	}else{
		s.cache.LDel(parts[0])
		r := Rsp{Key: parts[0], Value: "", Success: true}
		data, _ := json.Marshal(r)
		w.Write(data)
	}
}

func (s *apiServer) lDelRange(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path[len(LDelRange):], "/")
	if len(parts) != 1{
		r := Rsp{Key: "", Value: "", Success: false}
		data, _ := json.Marshal(r)
		http.Error(w, string(data), http.StatusBadRequest)
	}else{
		vars := r.URL.Query()
		beg, ok1 := vars["begIndex"]
		end, ok2 := vars["endIndex"]
		if ok1 == false || ok2 == false{
			r := Rsp{Key: "", Value: "", Success: false}
			data, _ := json.Marshal(r)
			http.Error(w, string(data), http.StatusBadRequest)
		}else{
			begInt, err1 := strconv.Atoi(beg[0])
			endInt, err2 := strconv.Atoi(end[0])

			if err1 != nil || err2 != nil{
				r := Rsp{Key: "", Value: "", Success: false}
				data, _ := json.Marshal(r)
				http.Error(w, string(data), http.StatusBadRequest)
			}else{
				err := s.cache.LDelRange(parts[0], int32(begInt), int32(endInt))
				if err == nil {
					r := Rsp{Key: parts[0], Value: "", Success: true}
					data, _ := json.Marshal(r)
					w.Write(data)
				}else{
					r := Rsp{Key: parts[0], Value: "", Success: false}
					data, _ := json.Marshal(r)
					w.Write(data)
				}
			}
		}
	}
}

func (s *apiServer) lDump(w http.ResponseWriter, r *http.Request){
	data, _ := s.cache.ListCaches()
	w.Write(data)
}

func (s *apiServer) sGet(w http.ResponseWriter, r *http.Request){
	parts := strings.Split(r.URL.Path[len(SGet):], "/")
	if len(parts) != 1{
		r := Rsp{Key: "", Value: "", Success: false}
		data, _ := json.Marshal(r)
		http.Error(w, string(data), http.StatusBadRequest)
	}else{
		v, err := s.cache.SGet(parts[0])
		if err == nil {
			r := Rsp{Key: parts[0], Value:v, Success: true}
			data, _ := json.Marshal(r)
			w.Write(data)
		}else{
			r := Rsp{Key: parts[0], Value: "", Success: false}
			data, _ := json.Marshal(r)
			w.Write(data)
		}
	}
}


func (s *apiServer) sPush(w http.ResponseWriter, r *http.Request){
	//fmt.Printf("key:%s\n",  vars["key"])
	//fmt.Printf("value:%s\n",  vars["value"])
	vars := r.URL.Query()
	key, ok1 := vars["key"]
	value, ok2 := vars["value"]
	expire, ok3 := vars["expire"]


	if ok1 == false {
		r := Rsp{Key: "", Value: "", Success: false}
		data, _ := json.Marshal(r)
		http.Error(w, string(data), http.StatusBadRequest)
		return
	}

	if ok2 == false {
		r := Rsp{Key: key[0], Value: "", Success: false}
		data, _ := json.Marshal(r)
		http.Error(w, string(data), http.StatusBadRequest)
		return
	}

	if ok3{
		int64, err := strconv.ParseInt(expire[0], 10, 64)
		if err == nil{
			s.cache.SPut(key[0], value, int64)
		}else{
			s.cache.SPut(key[0], value, cache.ExpireForever)
		}
	}else{
		s.cache.SPut(key[0], value, cache.ExpireForever)
	}

	str, _ := s.cache.SGet(key[0])
	rsp := Rsp{Key: key[0], Value:str, Success: true}
	data, _ := json.Marshal(rsp)
	w.Write(data)
}

func (s *apiServer) sDel(w http.ResponseWriter, r *http.Request){
	parts := strings.Split(r.URL.Path[len(SDel):], "/")
	if len(parts) != 1{
		r := Rsp{Key: parts[0], Value: "", Success: false}
		data, _ := json.Marshal(r)
		http.Error(w, string(data), http.StatusBadRequest)
	}else{
		s.cache.SDel(parts[0])
		r := Rsp{Key: parts[0], Value: "", Success: true}
		data, _ := json.Marshal(r)
		w.Write(data)
	}
}

func (s *apiServer) sDelMember(w http.ResponseWriter, r *http.Request){
	parts := strings.Split(r.URL.Path[len(SDelMember):], "/")
	if len(parts) != 1{
		r := Rsp{Key: parts[0], Value: "", Success: false}
		data, _ := json.Marshal(r)
		http.Error(w, string(data), http.StatusBadRequest)
	}else{
		vars := r.URL.Query()
		value, ok := vars["value"]
		if ok {
			s.cache.SDelMember(parts[0], value[0])
			r := Rsp{Key: parts[0], Value: value[0], Success: true}
			data, _ := json.Marshal(r)
			w.Write(data)
		}else {
			r := Rsp{Key: parts[0], Value: "", Success: false}
			data, _ := json.Marshal(r)
			http.Error(w, string(data), http.StatusBadRequest)
		}

	}
}

func (s *apiServer) sDump(w http.ResponseWriter, r *http.Request){
	data, _ := s.cache.SetCaches()
	w.Write(data)
}