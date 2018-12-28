package queue_kafka_test

import (
	"testing"
	"time"

	"github.com/carltd/glib/queue"
	"github.com/carltd/glib/queue/message"
	_ "github.com/carltd/glib/queue/queue_kafka"
	"github.com/carltd/glib/queue/testdata"
	"github.com/carltd/glib/queue/util"
)

const (
	driverName    = "kafka"
	dsn           = "kafka://127.0.0.1:9092,127.0.0.1:9093?broker_version=0.10.0.0"
	testTopic     = "test-topic"
	consumerGroup = "test"
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

	want := &testdata.Something{Name: "something", Age: 11}

	msg := &message.Message{
		Body: util.MustMessageBody(want),
	}

	qc, err := queue.NewPublisher(driverName, dsn)
	if err != nil {
		t.Fatal(err)
	}

	defer qc.Close()

	if err := qc.Publish(testTopic, msg); err != nil {
		t.Error(err)
	}
}

func TestKafkaSubscriber_NextMessage(t *testing.T) {
	want := &testdata.Something{
		Name: "something",
		Age:  11,
	}

	msg := &message.Message{
		Options:  map[string]string{"k1": "v1"},
		Priority: message.MsgPriority_PRIORITY0,
		Body:     util.MustMessageBody(want),
	}

	qc, err := queue.NewConsumer(driverName, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer qc.Close()

	sub, err := qc.Subscribe(testTopic, consumerGroup)
	if err != nil {
		t.Fatal(err)
	}

	m, err := sub.NextMessage(10 * time.Second)
	if err != nil {
		t.Fatal(err)
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
}

func BenchmarkKafkaProducer_Publish(b *testing.B) {
	dat := &testdata.Something{
		Name: "something",
		Age:  11,
	}

	msg := &message.Message{
		Body: util.MustMessageBody(dat),
	}

	qc, err := queue.NewPublisher(driverName, dsn)
	if err != nil {
		b.Fatal(err)
	}

	defer qc.Close()

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err = qc.Publish(testTopic, msg); err != nil {
				b.Error(err)
				b.FailNow()
			}
		}
	})
}

// FIXME: something blocking, need check
func BenchmarkKafkaSubscriber_NextMessage(b *testing.B) {
	qc, err := queue.NewConsumer(driverName, dsn)
	if err != nil {
		b.Fatal(err)
	}
	defer qc.Close()

	sub, err := qc.Subscribe(testTopic, consumerGroup)
	if err != nil {
		b.Fatal(err)
	}
	defer sub.Close()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err = sub.NextMessage(1 * time.Second)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
