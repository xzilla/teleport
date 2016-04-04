package database

import (
	"fmt"
	"bytes"
)

type Batch struct {
	Id     string `db:"id"`
	Status string `db:"status"`
	Data   *string `db:"data"`
}

func NewBatch(data []byte) *Batch {
	dataStr := string(data)

	return &Batch{
		Data: &dataStr,
		Status: "waiting_transmission",
	}
}

func (b *Batch) GetInsertQuery() string {
	return fmt.Sprintf(
		"INSERT INTO teleport.batch (status, data) VALUES ('%s', '%s')",
		b.Status,
		*b.Data,
	)
}

func (b *Batch) GetUpdateQuery() string {
	return fmt.Sprintf(
		"UPDAET teleport.batch SET status = '%s' WHERE id = '%s'",
		b.Status,
		b.Id,
	)
}

// Group all events 'waiting_batch' and create a batch with them.
func (db *Database) CreateBatchesFromEvents() error {
	// Get events waiting replication
	events, err := db.GetEvents("waiting_batch")

	if err != nil {
		return err
	}

	// Stop if there are no events
	if len(*events) == 0 {
		return nil
	}

	// Start a transaction
	tx := db.db.MustBegin()

	// Store batch data
	var batchBuffer bytes.Buffer

	for _, event := range *events {
		// Write event data to batch data
		batchBuffer.WriteString(event.String())
		batchBuffer.WriteString("\n")

		// Update event status to batched
		event.Status = "batched"
		tx.MustExec(event.GetUpdateQuery())
	}

	// Allocate a new batch
	batch := NewBatch(batchBuffer.Bytes())

	// Insert batch
	tx.MustExec(batch.GetInsertQuery())

	// Commit to database, returning errors
	return tx.Commit()
}
