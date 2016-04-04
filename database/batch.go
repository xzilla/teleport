package database

import (
	"bytes"
	"github.com/jmoiron/sqlx"
)

type Batch struct {
	Id     string  `db:"id" json:"id"`
	Status string  `db:"status" json:"status"`
	Data   *string `db:"data" json:"data"`
}

func NewBatch(data []byte) *Batch {
	dataStr := string(data)

	return &Batch{
		Data:   &dataStr,
		Status: "waiting_transmission",
	}
}

func (db *Database) GetBatches(status string) (*[]Batch, error) {
	var batches []Batch
	err := db.selectObjs(&batches, "SELECT * FROM teleport.batch WHERE status = $1 ORDER BY id ASC;", status)
	return &batches, err
}

func (b *Batch) InsertQuery(tx *sqlx.Tx) {
	tx.MustExec(
		"INSERT INTO teleport.batch (status, data) VALUES ($1, $2)",
		b.Status,
		b.Data,
	)
}

func (b *Batch) UpdateQuery(tx *sqlx.Tx) {
	tx.MustExec(
		"UPDATE teleport.batch SET status = $1 WHERE id = $2",
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
	tx := db.NewTransaction()

	// Store batch data
	var batchBuffer bytes.Buffer

	for _, event := range *events {
		// Write event data to batch data
		batchBuffer.WriteString(event.String())
		batchBuffer.WriteString("\n")

		// Update event status to batched
		event.Status = "batched"
		event.UpdateQuery(tx)
	}

	// Allocate a new batch
	batch := NewBatch(batchBuffer.Bytes())

	// Insert batch
	batch.InsertQuery(tx)

	// Commit to database, returning errors
	return tx.Commit()
}
