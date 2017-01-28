package manager_test

import (
	mg "github.com/pagarme/teleport/manager"
	"testing"
	"time"
)

type MockProcess struct{}

func (m *MockProcess) RunWatch() error {
	return nil
}

type StuckProcess struct{}

func (m *StuckProcess) RunWatch() error {
	select {}
}

func TestManagerStartsAndShutsdownProcesses(t *testing.T) {
	manager := mg.NewManager()

	go manager.Watch(&MockProcess{}, 10*time.Millisecond)
	go manager.Watch(&MockProcess{}, 10*time.Millisecond)

	select {
	case <-manager.AwaitShutdown():
	case <-time.After(50 * time.Millisecond):
		t.Errorf("Should only shutdown when AwaitShutdown completes")
	}
}

func TestManagerDoesntShutdownStuckProcess(t *testing.T) {
	manager := mg.NewManager()

	go manager.Watch(&MockProcess{}, 10*time.Millisecond)
	go manager.Watch(&StuckProcess{}, 10*time.Millisecond)

	time.Sleep(time.Second)

	select {
	case <-manager.AwaitShutdown():
		t.Errorf("A stuck process does not gracefully shuts down")
	case <-time.After(50 * time.Millisecond):
	}
}
