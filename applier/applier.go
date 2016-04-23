package applier

import (
	"github.com/jmoiron/sqlx"
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/database"
	"log"
	"time"
	"io"
)

type Applier struct {
	db *database.Database
	batchSize int
}

func New(db *database.Database, batchSize int) *Applier {
	return &Applier{
		db: db,
		batchSize: batchSize,
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

func (a *Applier) applyAction(act action.Action, tx *sqlx.Tx) error {
	// Execute action of the given event
	err := act.Execute(action.Context{
		Tx: tx,
		Db: a.db.Db,
	})

	if err != nil {
		log.Printf("Error applying action %#v: %v", act, err)
		return err
	}

	return nil
}

// Apply a batch
func (a *Applier) applyBatch(batch *database.Batch) error {
	// Start transaction
	tx := a.db.NewTransaction()

	// Update batch status based on a error
	updateBatchStatus := func(previousErr error) error {
		if previousErr != nil {
			// Create new transaction because the old one failed
			tx.Rollback()
			tx = a.db.NewTransaction()

			batch.Status = "waiting_apply"
			batch.WaitingReexecution = true
		} else {
			// Mark batch as applied (no error)
			batch.Status = "applied"
			batch.WaitingReexecution = false
		}

		err := batch.UpdateQuery(tx)

		if err != nil {
			return err
		}

		err = tx.Commit()

		if err != nil {
			return err
		}

		return previousErr
	}

	if batch.StorageType == "db" {
		actions, err := batch.GetActions()

		if err != nil {
			return err
		}

		for _, act := range actions {
			err := a.applyAction(act, tx)

			if err != nil {
				return updateBatchStatus(err)
			}
		}
	} else if batch.StorageType == "fs" {
		reader, file, err := batch.GetFileReader()
		defer file.Close()

		if err != nil {
			return updateBatchStatus(err)
		}

		var act action.Action

		act, err = batch.ReadAction(reader);

		for err == nil {
			err = a.applyAction(act, tx)

			if err != nil {
				return updateBatchStatus(err)
			}

			act, err = batch.ReadAction(reader);
		}

		if err != io.EOF {
			return updateBatchStatus(err)
		}
	}

	log.Printf("Applied batch: %v\n", batch)

	return updateBatchStatus(nil)
}
