package loader

import (
	"log"
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/database"
)

func (l *Loader) createDDLBatch() ([]*database.Batch, error) {
	schemas := make([]*database.Schema, 0)

	for _, schema := range l.db.Schemas {
		schemas = append(schemas, schema)
	}

	schemaName, _ := database.ParseTargetExpression(l.target.TargetExpression)

	// Build DDL object to diff from empty
	// schema to the current one
	ddl := &database.Ddl{
		[]*database.Schema{},
		schemas,
		l.db,
		schemaName,
	}

	actions := ddl.Diff()

	log.Printf("actions: %#v\n", actions)

	tx := l.db.NewTransaction()

	batches, err := l.batcher.CreateBatchesWithActions(
		map[string][]action.Action{
			l.targetName: actions,
		},
		tx,
	)

	if err != nil {
		return []*database.Batch{}, err
	}

	err = tx.Commit()

	if err != nil {
		return []*database.Batch{}, err
	}

	return batches, nil
}
