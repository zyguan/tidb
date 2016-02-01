package oracles

import (
	"fmt"
	"testing"
)

func TestRemoteOracle(t *testing.T) {
	oracle := NewRemoteOracle(fmt.Sprintf("%s:%d", "127.0.0.1", 4123))
	m := map[uint64]struct{}{}
	for i := 0; i < 100000; i++ {
		ts, err := oracle.GetTimestamp()
		if err != nil {
			t.Error(err)
		}
		m[ts] = struct{}{}
	}

	if len(m) != 100000 {
		t.Error("generated same ts, ", len(m))
	}
}
