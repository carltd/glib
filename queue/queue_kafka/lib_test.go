package queue_kafka_test

import (
	"github.com/carltd/glib/queue"
	"github.com/carltd/glib/queue/message"
	_ "github.com/carltd/glib/queue/queue_kafka"
	"github.com/carltd/glib/queue/testdata"
	"github.com/carltd/glib/queue/util"
	"testing"
	"time"
)

const (
	driverName  = "kafka"
	dsn         = "kafka://127.0.0.1:9092?broker_version=2.1.0"
	testSubject = "test-topic"
)

func TestDrivers(t *testing.T) {
	ds := queue.Drivers()
	if len(ds) != 1 {
		t.Errorf("driver want 1, got %d", len(ds))
	}
	if ds[0] != driverName {
		t.Errorf("driver's name want %s, got %s", driverName, ds[0])
	}
}

func TestNewPublisher(t *testing.T) {

	want := &testdata.Something{
		Name: "something",
		Age:  11,
	}

	msg := &message.Message{
		Options:  map[string]string{"k1": "v1"},
		Priority: message.MsgPriority_PRIORITY0,
		Body:     util.MustMessageBody(want),
	}

	qc, err := queue.NewPublisher(driverName, dsn)
	if err != nil {
		t.Fatal(err)
	}

	defer qc.Close()

	if err := qc.Publish(testSubject, msg); err != nil {
		t.Error(err)
	}

	t.Run("subscribe", func(t *testing.T) {
		qc, err := queue.NewConsumer(driverName, dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer qc.Close()

		sub, err := qc.Subscribe(testSubject, "test")
		if err != nil {
			t.Fatal(err)
		}

		m, err := sub.NextMessage(10 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if m.Options == nil {
			t.Fatal("options got nil")
		} else {
			if m.Options["k1"] != msg.Options["k1"] {
				t.Fatalf("options field want %v got %v", msg.Options["k1"], m.Options["k1"])
			}
		}

		got := testdata.Something{}
		if err := util.FromMessageBody(m.Body, &got); err != nil {
			t.Fatal(err)
		}

		if msg.MessageId != m.MessageId {
			t.Fatalf("message id: want %#x, got %#x", msg.MessageId, m.MessageId)
		}

		if want.Name != got.Name {
			t.Fatalf("name: want %v, got %v", want.Name, got.Name)
		}

		if want.Age != got.Age {
			t.Fatalf("Age: want %v, got %v", want.Age, got.Age)
		}
	})
}
