package queue_redis_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/carltd/glib/queue"
	"github.com/carltd/glib/queue/message"
	_ "github.com/carltd/glib/queue/queue_redis"
	"github.com/carltd/glib/queue/testdata"
	"github.com/carltd/glib/queue/util"
)

const (
	driverName  = "redis"
	redisDSN    = "redis://:123456@127.0.0.1:16379/1?maxIdle=10&maxActive=10&idleTimeout=3"
	testSubject = "testSubject"
)

func TestNewPublisher(t *testing.T) {

	want := &testdata.Something{
		Name: "something",
		Age:  11,
	}

	msg := &message.Message{
		Priority: message.MsgPriority_PRIORITY0,
		Body:     util.MustMessageBody(want),
	}

	var canPublish = make(chan bool)

	go func() {
		qc, err := queue.NewConsumer(driverName, redisDSN)
		if err != nil {
			t.Fatal(err)
		}

		sub, err := qc.Subscribe(testSubject, "test")
		if err != nil {
			t.Fatal(err)
		}

		defer sub.Close()
		canPublish <- true
		defer close(canPublish)

		m, err := sub.NextMessage(3 * time.Second)
		if err != nil {
			t.Fatal(err)
		}

		got := &testdata.Something{}
		if err := util.FromMessageBody(m.Body, got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(want, got) {
			t.Errorf("message=%#v, want=%#v", got, want)
		}

		if _, err = sub.NextMessage(time.Second); err != queue.ErrTimeout {
			t.Errorf("want (%v), got (%v)", queue.ErrTimeout, err)
		}
	}()

	qp, err := queue.NewPublisher(driverName, redisDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer qp.Close()
	<-canPublish
	if err := qp.Publish(testSubject, msg); err != nil {
		t.Error(err)
	}
	<-canPublish
}

func TestDrivers(t *testing.T) {
	ds := queue.Drivers()
	if len(ds) != 1 {
		t.Errorf("driver want 1, got %d", len(ds))
	}
	if ds[0] != driverName {
		t.Errorf("driver's name want %s, got %s", driverName, ds[0])
	}
}

func TestRedisQueueConn_Enqueue(t *testing.T) {
	qp, err := queue.NewPublisher(driverName, redisDSN)
	if err != nil {
		t.Fatal(err)
	}

	defer qp.Close()
	want := &testdata.Something{
		Name: "something",
		Age:  11,
	}

	msg := &message.Message{
		Priority: message.MsgPriority_PRIORITY0,
		Body:     util.MustMessageBody(want),
	}
	if err := qp.Enqueue(testSubject, msg); err != nil {
		t.Error(err)
	}
}

func TestRedisQueueConn_Dequeue(t *testing.T) {
	want := &testdata.Something{
		Name: "something",
		Age:  11,
	}
	msg := &message.Message{
		Priority: message.MsgPriority_PRIORITY0,
		Body:     util.MustMessageBody(want),
	}

	// enqueue a message at first
	qp, err := queue.NewPublisher(driverName, redisDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer qp.Close()
	if err = qp.Enqueue(testSubject, msg); err != nil {
		t.Fatal(err)
	}

	// dequeue a message
	qc, err := queue.NewConsumer(driverName, redisDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer qc.Close()

	got := testdata.Something{}
	if _, err = qc.Dequeue(testSubject, "test", 10*time.Second, &got); err != nil {
		t.Fatal(err)
	}

	if want.Name != got.Name {
		t.Errorf("want %s, got %s", want.Name, got.Name)
	}
	if want.Age != got.Age {
		t.Errorf("want %v, got %v", want.Age, got.Age)
	}
}
