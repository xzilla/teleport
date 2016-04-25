package applier

import (
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
		batches, err := a.db.GetBatches("waiting_apply", "")

		if err != nil {
			log.Printf("Error fetching batches to apply! %v\n", err)
		} else {
			for _, batch := range batches {
				shouldContinue, err := a.applyBatch(batch)

				if err != nil {
					log.Printf("Error applying batch %s: %v\n", batch.Id, err)
				}

				if !shouldContinue {
					break
				}
			}
		}

		time.Sleep(sleepTime)
	}
}

func (a *Applier) applyAction(act action.Action, context *action.Context) error {
	// Execute action of the given event
	err := act.Execute(context)

	if err != nil {
		log.Printf("Error applying action %#v: %v", act, err)
		return err
	}

	return nil
}

// Apply a batch
func (a *Applier) applyBatch(batch *database.Batch) (bool, error) {
	if batch.DataStatus != "transmitted" {
		return false, nil
	}

	// Start transaction
	tx := a.db.NewTransaction()

	// Update batch status based on a error
	updateBatchStatus := func(previousErr error) (bool, error) {
		if previousErr != nil {
			// Create new transaction because the old one failed
			tx.Rollback()
			tx = a.db.NewTransaction()

			batch.Status = "waiting_apply"
			batch.DataStatus = ""
			batch.WaitingReexecution = true
		} else {
			// Mark batch as applied (no error)
			batch.Status = "applied"
			batch.DataStatus = "applied"
			batch.WaitingReexecution = false
		}

		err := batch.UpdateQuery(tx)

		if err != nil {
			return false, err
		}

		err = tx.Commit()

		if err != nil {
			return false, err
		}

		return true, previousErr
	}

	if batch.StorageType == "db" {
		actions, err := batch.GetActions()

		if err != nil {
			return false, err
		}

		context := action.NewContext(tx, a.db.Db)

		for _, act := range actions {
			err := a.applyAction(act, context)

			if err != nil {
				return updateBatchStatus(err)
			}
		}
	} else if batch.StorageType == "fs" {
		reader, file, err := batch.GetFileReader()

		if err != nil {
			return updateBatchStatus(err)
		}

		defer file.Close()

		var act action.Action
		currentStatement := 0
		currentBatchSize := 0
		previousStatement := batch.LastExecutedStatement

		currentContext := action.NewContext(tx, a.db.Db)
		act, err = batch.ReadAction(reader);

		for err == nil {
			// Increment current statement
			currentStatement += 1

			// Start applying from previous stop point
			if currentStatement > previousStatement {
				err = a.applyAction(act, currentContext)

				if err != nil {
					return updateBatchStatus(err)
				}

				batch.LastExecutedStatement += 1
				currentBatchSize += 1

				if currentBatchSize >= a.batchSize {
					// Current batch reached the maximum size.
					// Commit batch to database.
					err = batch.UpdateQuery(tx)

					if err != nil {
						return false, err
					}

					err = tx.Commit()

					if err != nil {
						return false, err
					}

					// Restart transaction
					tx = a.db.NewTransaction()

					currentContext = action.NewContext(tx, a.db.Db)

					// Reset batch size
					currentBatchSize = 0
				}
			}

			// Read next action
			act, err = batch.ReadAction(reader);
		}

		if err != io.EOF {
			return updateBatchStatus(err)
		}
	}

	log.Printf("Applied batch: %v\n", batch)

	return updateBatchStatus(nil)
}
