package batch

import (
	"github.com/pagarme/teleport/server/httputils"
	// "github.com/pagarme/teleport/database"
	"github.com/pagarme/teleport/server/router"
	"net/http"
)

type batchRouter struct {
	routes []router.Route
}

func New() *batchRouter {
	b := &batchRouter{}
	b.initRoutes()
	return b
}

func (b *batchRouter) create(w http.ResponseWriter, r *http.Request) error {
	return httputils.WriteJSON(w, http.StatusOK, "Creating new batch.")
}

func (b *batchRouter) Routes() []router.Route {
	return b.routes
}

func (b *batchRouter) initRoutes() {
	b.routes = []router.Route{
		router.NewPostRoute("/batches", b.create),
	}
}

