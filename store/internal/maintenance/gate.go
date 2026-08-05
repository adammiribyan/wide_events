package maintenance

import (
	"context"
	"errors"
	"sync"
)

var ErrMaintenance = errors.New("maintenance in progress")

type gate struct {
	mutex         sync.Mutex
	writesClosed  bool
	queriesClosed bool
	writes        int
	queries       int
	changed       chan struct{}
}

func newGate() *gate { return &gate{changed: make(chan struct{})} }

func (gate *gate) admitWrite(context.Context) (func(), error) {
	gate.mutex.Lock()
	defer gate.mutex.Unlock()
	if gate.writesClosed {
		return nil, ErrMaintenance
	}
	gate.writes++
	return gate.releaseFunc(true), nil
}

func (gate *gate) admitQuery(context.Context) (func(), error) {
	gate.mutex.Lock()
	defer gate.mutex.Unlock()
	if gate.queriesClosed {
		return nil, ErrMaintenance
	}
	gate.queries++
	return gate.releaseFunc(false), nil
}

func (gate *gate) releaseFunc(write bool) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			gate.mutex.Lock()
			if write {
				gate.writes--
			} else {
				gate.queries--
			}
			gate.notifyLocked()
			gate.mutex.Unlock()
		})
	}
}

func (gate *gate) closeWrites(ctx context.Context) error {
	return gate.closeAndDrain(ctx, false)
}

func (gate *gate) closeAll(ctx context.Context) error {
	return gate.closeAndDrain(ctx, true)
}

func (gate *gate) closeAndDrain(ctx context.Context, queries bool) error {
	gate.mutex.Lock()
	gate.writesClosed = true
	if queries {
		gate.queriesClosed = true
	}
	for gate.writes > 0 || queries && gate.queries > 0 {
		changed := gate.changed
		gate.mutex.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			gate.open()
			return ctx.Err()
		}
		gate.mutex.Lock()
	}
	gate.mutex.Unlock()
	return nil
}

func (gate *gate) open() {
	gate.mutex.Lock()
	gate.writesClosed = false
	gate.queriesClosed = false
	gate.notifyLocked()
	gate.mutex.Unlock()
}

func (gate *gate) notifyLocked() {
	close(gate.changed)
	gate.changed = make(chan struct{})
}
