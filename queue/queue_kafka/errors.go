package queue_kafka

import "errors"

// ErrNextMessageTimeOut  next message timeout
var ErrNextMessageTimeOut = errors.New("consumer get message timeout")
