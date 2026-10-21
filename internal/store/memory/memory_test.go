package memory

import (
	"testing"

	"sis2_highload/internal/store/storetest"
)

type counted struct{ *Store }

func (c counted) RowCount(*testing.T) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, byChannel := range c.data {
		n += len(byChannel)
	}
	return n
}

func TestStore(t *testing.T) {
	storetest.Run(t, func(*testing.T) storetest.Store { return counted{New()} })
}
