package batcher

import (
	"github.com/pagarme/teleport/database"
	"github.com/pagarme/teleport/client"
	"bytes"
	"log"
	"time"
)

type Batcher struct {
	db *database.Database
	targets map[string]*client.Client
}

func New(db *database.Database, targets map[string]*client.Client) *Batcher {
	return &Batcher{
		db: db,
		targets: targets,
	}
}

// Every sleepTime interval, create a batch with unbatched events
func (b *Batcher) Watch(sleepTime time.Duration) {
	for {
		err := b.createBatches()

		if err != nil {
			log.Printf("Error creating batch! %v\n", err)
		}

		time.Sleep(sleepTime)
	}
}

// Group all events 'waiting_batch' and create a batch with them.
func (b *Batcher) createBatches() error {
	// Get events waiting replication
	events, err := b.db.GetEvents("waiting_batch")

	if err != nil {
		return err
	}

	// Stop if there are no events
	if len(events) == 0 {
		return nil
	}

	for targetName, _ := range b.targets {
		// Start a transaction
		tx := b.db.NewTransaction()

		// Store batch data
		var batchBuffer bytes.Buffer

		for _, event := range events {
			// Write event data to batch data
			batchBuffer.WriteString(event.String())
			batchBuffer.WriteString("\n")

			// Update event status to batched
			event.Status = "batched"
			event.UpdateQuery(tx)
		}

		// Allocate a new batch
		batch := database.NewBatch(batchBuffer.Bytes())

		// Set source and target
		batch.Source = b.db.Name
		batch.Target = targetName

		// Insert batch
		batch.InsertQuery(tx)

		// Commit to database, returning errors
		err := tx.Commit()

		if err != nil {
			return err
		}
	}

	return nil
}
