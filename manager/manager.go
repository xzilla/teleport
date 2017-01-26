package manager

type link struct {
	Shutdown <-chan struct{}
	closed   chan<- interface{}
}

func NewLink(shutdown chan struct{}, closed chan<- interface{}) *link {
	return &link{shutdown, closed}
}

func (m *link) Close() {
	m.closed <- nil
}
