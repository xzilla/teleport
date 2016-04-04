package server

import (
	"github.com/pagarme/teleport/config"
	"log"
)

// Define HTTP server
type Server struct {
	HTTP config.HTTP
}

func New(config config.HTTP) *Server {
	return &Server{
		HTTP: config,
	}
}

// Start HTTP server
func (s *Server) Start() error {
	log.Printf("Started server on port %d", s.HTTP.Port)

	return nil
}
