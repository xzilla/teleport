package manager

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/pagarme/teleport/manager/link"
)

type Process interface {
	RunWatch() error
}

type Manager interface {
	Watch(process Process, duration time.Duration)
	AwaitShutdown() <-chan interface{}
}

type manager struct {
	toBeClosed       int
	internalShutdown chan struct{}
	closed           chan interface{}
}

func (m *manager) Watch(process Process, duration time.Duration) {
	lk := link.NewLink(m.internalShutdown, m.closed)
	m.toBeClosed = m.toBeClosed + 1

	for {
		select {
		case <-lk.AwaitShutdown():
			lk.Close()
			return
		default:
			err := process.RunWatch()

			if err != nil {
				log.Errorf("Error during RunWatch: %v", err)
			}

			time.Sleep(duration)
		}
	}
}

func (m *manager) AwaitShutdown() <-chan interface{} {
	externalShutdown := make(chan interface{})

	go func() {
		close(m.internalShutdown)

		alreadyClosed := 0

		for alreadyClosed < m.toBeClosed {
			<-m.closed
			alreadyClosed = alreadyClosed + 1
		}

		externalShutdown <- nil
	}()

	return externalShutdown
}

func NewManager() Manager {
	return &manager{
		0,
		make(chan struct{}),
		make(chan interface{}),
	}
}
