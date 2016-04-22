package applier

import (
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/database"
	"github.com/jmoiron/sqlx"
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
				err := a.applyBatch(batch)

				if err != nil {
					log.Printf("Error applying batch %s: %v\n", batch.Id, err)
					break
				}
			}
		}

		time.Sleep(sleepTime)
	}
}

func (a *Applier) applyEvent(event *database.Event, tx *sqlx.Tx) error {
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

	return nil
}

// Apply a batch
func (a *Applier) applyBatch(batch *database.Batch) error {
	// Start transaction
	tx := a.db.NewTransaction()

	events, err := batch.GetEvents()

	if err != nil {
		return err
	}

	if batch.StorageType == "db" {
		for _, event := range events {
			err := a.applyEvent(&event, tx)

			if err != nil {
				return err
			}
		}
	} else if batch.StorageType == "fs" {
		scanner, file, err := batch.GetFileScanner()
		defer file.Close()

		if err != nil {
			return err
		}

		for scanner.Scan() {
			event := batch.EventFromData(scanner.Text())

			if event == nil {
				continue
			}

			err := a.applyEvent(event, tx)

			if err != nil {
				return err
			}
		}

		if err := scanner.Err(); err != nil {
			return err
		}
	}

	log.Printf("Applied batch: %v\n", batch)

	// Mark batch as applied
	batch.Status = "applied"
	batch.UpdateQuery(tx)

	return tx.Commit()
}
