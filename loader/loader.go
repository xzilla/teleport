package loader

import (
	"github.com/pagarme/teleport/batcher"
	"github.com/pagarme/teleport/client"
	"github.com/pagarme/teleport/database"
)

type Loader struct {
	BatchSize  int
	db         *database.Database
	target     *client.Client
	targetName string
	batcher    *batcher.Batcher
}

func New(db *database.Database, target *client.Client, targetName string, batchSize, maxEventsPerBatch int) *Loader {
	batcher := batcher.New(db, map[string]*client.Client{
		targetName: target,
	}, maxEventsPerBatch)

	return &Loader{
		db:         db,
		target:     target,
		targetName: targetName,
		batcher:    batcher,
		BatchSize:  batchSize,
	}
}

func (l *Loader) Load() error {
	events, err := l.db.GetEvents(nil, "building", -1)

	if err != nil {
		return err
	}

	eventBatches, err := l.getDMLBatchEvents(events)

	if err != nil {
		return err
	}

	if len(eventBatches) == 0 {
		// Start new initial load
		_, err = l.createDDLBatch()

		if err != nil {
			return err
		}

		// Create DML events
		eventBatches, err = l.createDMLEvents()

		if err != nil {
			return err
		}
	}

	// Resume initial load (from existing events or new events)
	return l.resumeDMLEvents(eventBatches)
}
