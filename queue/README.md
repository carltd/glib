## Description
Queue Lib for glib, internal support Redis Only.

## Install
  `go get github.com/carltd/glib/v2/queue`

## Usage

#### Publisher
```go
import (
    "github.com/carltd/glib/v2/queue/message"
    
    "github.com/carltd/glib/v2/queue"
    _ "github.com/carltd/glib/v2/queue/queue_redis"
)

pub, _ := queue.NewPublisher("redis", "redis://127.0.0.1:6379")
pub.Publish("subject", &message.Message{
    Body: util.MustMessageBody(nil, /* point to your protobuffer struct */ ),
})
pub.Close()
```

#### Consumer
```go
import (
    "github.com/carltd/glib/v2/queue/message"
    
    "github.com/carltd/glib/v2/queue"
    _ "github.com/carltd/glib/v2/queue/queue_redis"
)

con, _ := queue.NewConsumer("redis", "redis://127.0.0.1:6379")
sub,_ := con.Subscribe("subject", "cluster-group")
msg, _ := sub.NextMessage(time.Second)
// logic for msg
con.Close()
```
