package applier

import (
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
				err := a.Apply(&batch)

				if err != nil {
					log.Printf("Error applying batch %s: %v\n", batch.Id, err)
				}
			}
		}

		time.Sleep(sleepTime)
	}
}

// Apply a batch
func (a *Applier) Apply(batch *database.Batch) error {
	log.Printf("applying batch %v\n", batch)
	return nil
}

func (a *Applier) markBatchApplied(batch *database.Batch) error {
	// Start transaction
	tx := a.db.NewTransaction()

	// Update batch to transmitted
	batch.Status = "applied"

	// Insert
	batch.UpdateQuery(tx)

	// Commit transaction
	return tx.Commit()
}
