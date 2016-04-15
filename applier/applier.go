package applier

import (
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/database"
	"log"
	"time"
)

type Applier struct {
	db *database.Database
}

func New(db *database.Database) *Applier {
	return &Applier{
		db: db,
	}
}

// Transmit batches
func (a *Applier) Watch(sleepTime time.Duration) {
	for {
		batches, err := a.db.GetBatches("waiting_apply")

		if err != nil {
			log.Printf("Error fetching batches to apply! %v\n", err)
		} else {
			for _, batch := range batches {
				err := a.applyBatch(&batch)

				if err != nil {
					log.Printf("Error applying batch %s: %v\n", batch.Id, err)
				}
			}
		}

		time.Sleep(sleepTime)
	}
}

// Apply a batch
func (a *Applier) applyBatch(batch *database.Batch) error {
	events := batch.GetEvents()

	// Start transaction
	tx := a.db.NewTransaction()

	log.Printf("applying batch %v\n", batch)

	for _, event := range events {
		currentAction, err := event.GetActionFromData()

		if err != nil {
			return err
		}

		// Execute action of the given event
		err = currentAction.Execute(action.Context{
			Tx: tx,
			Db: a.db.Db,
		})

		if err != nil {
			log.Printf("Error applying event %d: %v", event.Id, err)
			return err
		}
	}

	// Mark batch as applied
	batch.Status = "applied"
	batch.UpdateQuery(tx)

	return tx.Commit()
}
