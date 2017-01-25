package manager

import (
	"testing"
	"time"
)

func launchManager(manager *manager) {
	select {
	case <-manager.Shutdown:
		manager.Close()
	}
}

func TestClosesGoroutinesGracefully(t *testing.T) {
	shutdown := make(chan struct{})
	closed := make(chan interface{})

	manager := New(shutdown, closed)

	go launchManager(manager)
	go close(shutdown)

	select {
	case <-closed:
	case <-time.After(50 * time.Millisecond):
		t.Errorf("Timeout passed. Should have gracefully shutdown.")
	}
}

func TestShouldTimeoutWHenShutdownChannelIsNotClosed(t *testing.T) {
	shutdown := make(chan struct{})
	closed := make(chan interface{})

	manager := New(shutdown, closed)

	go launchManager(manager)

	select {
	case <-closed:
		t.Errorf("Should only be closed when the shutdown channel is closed.")
	case <-time.After(50 * time.Millisecond):
	}
}
