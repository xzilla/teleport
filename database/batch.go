package database

import (
	"fmt"
	"bytes"
)

type Batch struct {
	Id     *string `db:"id"`
	Data   *string `db:"data"`
	// Source *string `db:"source"`
	// Target *string `db:"target"`
}

func NewBatch(data []byte) *Batch {
	dataStr := string(data)

	return &Batch{
		Data: &dataStr,
	}
}

func (b *Batch) GetInsertQuery() string {
	return fmt.Sprintf(
		"INSERT INTO teleport.batch (data) VALUES ('%s')",
		*b.Data,
	)
}

// Group all events 'waiting_replication' and create a batch with them.
func (db *Database) CreateBatchesFromEvents() error {
	// Get events waiting replication
	events, err := db.GetEvents("waiting_replication")

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

		// Update event status to replicated
		event.Status = "replicated"
		tx.MustExec(event.GetUpdateQuery())
	}

	// Allocate a new batch
	batch := NewBatch(batchBuffer.Bytes())

	// Insert batch
	tx.MustExec(batch.GetInsertQuery())

	// Commit to database, returning errors
	return tx.Commit()
}
