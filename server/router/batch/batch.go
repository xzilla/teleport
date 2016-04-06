package batch

import (
	"encoding/json"
	"github.com/pagarme/teleport/database"
	"github.com/pagarme/teleport/server/httputils"
	"github.com/pagarme/teleport/server/router"
	"net/http"
	"log"
)

type batchRouter struct {
	routes []router.Route
	db     *database.Database
}

func New(db *database.Database) *batchRouter {
	b := &batchRouter{}
	b.db = db
	b.initRoutes()
	return b
}

func (b *batchRouter) create(w http.ResponseWriter, r *http.Request) error {
	// Parse batch data from request
	var newBatch database.Batch
	json.NewDecoder(r.Body).Decode(&newBatch)

	// Start transaction
	tx := b.db.NewTransaction()

	// New batches are waiting_apply always
	newBatch.Status = "waiting_apply"

	// Insert
	newBatch.InsertQuery(tx)

	log.Printf("Received batch: %v\n", newBatch)

	// Commit transaction
	err := tx.Commit()

	if err != nil {
		return err
	}

	// Respond HTTP OK
	return httputils.WriteJSON(w, http.StatusOK, nil)
}

func (b *batchRouter) Routes() []router.Route {
	return b.routes
}

func (b *batchRouter) initRoutes() {
	b.routes = []router.Route{
		router.NewPostRoute("/batches", b.create),
	}
}
