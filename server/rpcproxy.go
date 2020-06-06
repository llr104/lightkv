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
}
