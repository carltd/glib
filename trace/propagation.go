package gtrace

import (
	"encoding/binary"
	"github.com/openzipkin/zipkin-go/model"
)

func marshal(sc model.SpanContext) []byte {
	if sc == (model.SpanContext{}) {
		return nil
	}

	var b [27]byte
	binary.BigEndian.PutUint64(b[2:10], sc.TraceID.High)
	binary.BigEndian.PutUint64(b[10:18], sc.TraceID.Low)
	b[18] = 1
	binary.BigEndian.PutUint64(b[19:27], uint64(sc.ID))
	return b[:]
}

// FromBinary returns the SpanContext represented by b.
//
// If b has an unsupported version ID or contains no TraceID, FromBinary
// returns with ok==false.
func unmarshal(b []byte) (sc model.SpanContext, ok bool) {
	if len(b) == 0 || b[0] != 0 {
		return model.SpanContext{}, false
	}
	b = b[1:]
	if len(b) >= 17 && b[0] == 0 {
		sc.TraceID.High = binary.BigEndian.Uint64(b[1:9])
		sc.TraceID.Low = binary.BigEndian.Uint64(b[9:17])
		b = b[17:]
	} else {
		return model.SpanContext{}, false
	}

	if len(b) >= 9 && b[0] == 1 {
		sc.ID = model.ID(binary.BigEndian.Uint64(b[1:]))
	}

	return sc, true
}
