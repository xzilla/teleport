package server

import (
	"fmt"
	"github.com/gorilla/mux"
	"github.com/pagarme/teleport/config"
	"github.com/pagarme/teleport/database"
	"github.com/pagarme/teleport/server/httputils"
	"github.com/pagarme/teleport/server/router"
	batchrouter "github.com/pagarme/teleport/server/router/batch"
	statusrouter "github.com/pagarme/teleport/server/router/status"
	"log"
	"net/http"
	"time"
)

type Server struct {
	HTTP           config.HTTP
	internalRouter *mux.Router
}

func New(db *database.Database, config config.HTTP) *Server {
	server := &Server{
		HTTP:           config,
		internalRouter: mux.NewRouter().StrictSlash(true),
	}

	server.AddRouter(batchrouter.New(db))
	server.AddRouter(statusrouter.New())

	return server
}

// Start HTTP server
func (s *Server) Start() error {
	hostStr := fmt.Sprintf(
		"%v:%v",
		s.HTTP.Hostname,
		s.HTTP.Port,
	)

	log.Printf("Starting server on %s", hostStr)

	return http.ListenAndServe(hostStr, s.GetDefaultHandler())
}

func (s *Server) GetDefaultHandler() http.Handler {
	return s.internalRouter
}

func (s *Server) handlerForRoute(route router.Route) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler := route.Handler

		// for _, middleware := range s.middlewares {
		// 	handler = middleware.Handler(handler, route)
		// }

		start := time.Now()

		if err := handler(w, r); err != nil {
			httputils.WriteError(w, err)
		}

		log.Printf(
			"%s\t%s\t%s",
			r.Method,
			r.RequestURI,
			time.Since(start),
		)
	})
}

func (s *Server) AddRouter(r router.Router) {
	for _, route := range r.Routes() {
		s.internalRouter.
			Methods(route.Method).
			Path(route.Path).
			Name(route.Path).
			Handler(s.handlerForRoute(route))
	}
}
