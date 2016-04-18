package loader

import (
	// "fmt"
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/batcher"
	"github.com/pagarme/teleport/client"
	"github.com/pagarme/teleport/database"
)

type Loader struct {
	db         *database.Database
	target     *client.Client
	targetName string
	batcher    *batcher.Batcher
}

func New(db *database.Database, target *client.Client, targetName string) *Loader {
	batcher := batcher.New(db, map[string]*client.Client{
		targetName: target,
	})

	return &Loader{
		db:         db,
		target:     target,
		targetName: targetName,
		batcher:    batcher,
	}
}

func (l *Loader) CreateEvents() error {
	return l.createDDLBatch()
}

func (l *Loader) createDDLBatch() error {
	schemas := make([]*database.Schema, 0)

	for _, schema := range l.db.Schemas {
		schemas = append(schemas, schema)
	}

	// fmt.Printf("public classes: %#v\n", l.db.Schemas["public"])

	// Build DDL object to diff from empty
	// schema to the current one
	ddl := &database.Ddl{
		[]*database.Schema{},
		schemas,
		l.db,
	}

	actions := ddl.Diff()

	// fmt.Printf("actions: %#v\n", actions)

	tx := l.db.NewTransaction()

	// Build a DDL event to hold the actions
	event := &database.Event{
		Kind:          "ddl",
		Status:        "waiting_batch",
		TriggerTag:    l.target.TargetExpression,
		TriggerEvent:  "ddl_initial_load",
		TransactionId: "0",
		Data:          nil,
	}

	event.InsertQuery(tx)

	_, _, err := l.batcher.CreateBatchesWithActions(
		map[database.Event][]action.Action{
			*event: actions,
		},
	)

	if err != nil {
		return tx.Commit()
	}

	return err
}
