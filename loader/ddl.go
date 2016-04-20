package loader

import (
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/database"
)

func (l *Loader) createDDLBatch() ([]*database.Batch, error) {
	schemas := make([]*database.Schema, 0)

	for _, schema := range l.db.Schemas {
		schemas = append(schemas, schema)
	}

	// Build DDL object to diff from empty
	// schema to the current one
	ddl := &database.Ddl{
		[]*database.Schema{},
		schemas,
		l.db,
	}

	actions := ddl.Diff()

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

	_, batches, err := l.batcher.CreateBatchesWithActions(
		map[database.Event][]action.Action{
			*event: actions,
		},
	)

	if err == nil {
		return []*database.Batch{}, err
	}

	return batches, nil
}
