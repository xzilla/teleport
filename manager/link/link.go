package link

type Link interface {
	Shutdown() <-chan struct{}
	Close()
}

type link struct {
	shutdown <-chan struct{}
	closed   chan<- interface{}
}

func NewLink(shutdown <-chan struct{}, closed chan<- interface{}) Link {
	return &link{shutdown, closed}
}

func (m *link) Close() {
	m.closed <- nil
}

func (m *link) Shutdown() <-chan struct{} {
	return m.shutdown
}
