package server

import (
	"context"
	bridge "github.com/llr104/lightkv/pb"
)


type rpcProxy struct {
	sendCancel context.CancelFunc
	recvCancel context.CancelFunc
	sendChan chan bridge.PublishRsp
	watchKey map[string]string
	watchMap map[string]map[string]string
	watchList map[string]string
	watchSet map[string]string
}

func newProxy() *rpcProxy{
	return &rpcProxy{
					watchKey:make(map[string]string),
					watchMap:make(map[string]map[string]string),
					watchList:make(map[string]string),
					watchSet:make(map[string]string),
	}
}
