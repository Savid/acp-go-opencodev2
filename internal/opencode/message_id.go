package opencode

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

var messageIDs struct {
	sync.Mutex
	millis  int64
	counter uint64
}

// NewMessageID uses the native timestamp layout so later CLI messages sort after it.
func NewMessageID() string {
	messageIDs.Lock()
	defer messageIDs.Unlock()

	millis := time.Now().UnixMilli()
	if millis != messageIDs.millis {
		messageIDs.millis = millis
		messageIDs.counter = 0
	}

	messageIDs.counter++
	stamp := (uint64(millis)*4096 + messageIDs.counter) & ((1 << 48) - 1)

	data := make([]byte, 6)
	for index := 5; index >= 0; index-- {
		data[index] = byte(stamp & 0xff)
		stamp >>= 8
	}

	const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

	tail := make([]byte, 14)

	_, _ = rand.Read(tail)
	for index, value := range tail {
		tail[index] = alphabet[int(value)%len(alphabet)]
	}

	return "msg_" + hex.EncodeToString(data) + string(tail)
}
