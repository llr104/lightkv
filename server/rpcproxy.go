package server

import (
	"context"
	bridge "lightkv/pb"
)

type rpcProxy struct {
	sendCancel context.CancelFunc
	recvCancel context.CancelFunc
	sendChan chan bridge.PublishRsp
	watchKey map[string]string
}
