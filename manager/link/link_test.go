package link_test

import (
	. "github.com/pagarme/teleport/manager/link"
	"testing"
	"time"
)

func launchLink(link Link) {
	select {
	case <-link.Shutdown():
		link.Close()
	}
}

func TestClosesGoroutinesGracefully(t *testing.T) {
	shutdown := make(chan struct{})
	closed := make(chan interface{})

	link := NewLink(shutdown, closed)

	go launchLink(link)
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

	link := NewLink(shutdown, closed)

	go launchLink(link)

	select {
	case <-closed:
		t.Errorf("Should only be closed when the shutdown channel is closed.")
	case <-time.After(50 * time.Millisecond):
	}
}
