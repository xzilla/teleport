package manager

type manager struct {
	Shutdown <-chan struct{}
	closed   chan<- interface{}
}

func New(shutdown chan struct{}, closed chan<- interface{}) *manager {
	return &manager{shutdown, closed}
}

func (m *manager) Close() {
	m.closed <- nil
}
