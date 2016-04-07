package database

import (
	"bytes"
	"github.com/jmoiron/sqlx"
	"strings"
)

type Batch struct {
	Id     string  `db:"id" json:"id"`
	Status string  `db:"status" json:"status"`
	Source string  `db:"source" json:"source"`
	Target string  `db:"target" json:"target"`
	Data   *string `db:"data" json:"data"`
}

func NewBatch() *Batch {
	return &Batch{
		Status: "waiting_transmission",
	}
}

func (db *Database) GetBatches(status string) ([]Batch, error) {
	var batches []Batch
	err := db.selectObjs(&batches, "SELECT * FROM teleport.batch WHERE status = $1 ORDER BY id ASC;", status)
	return batches, err
}

func (b *Batch) InsertQuery(tx *sqlx.Tx) {
	args := make([]interface{}, 0)
	var query string

	// If there's no id, insert without id
	if b.Id == "" {
		query = "INSERT INTO teleport.batch (status, data, source, target) VALUES ($1, $2, $3, $4)"
	} else {
		query = "INSERT INTO teleport.batch (id, status, data, source, target) VALUES ($1, $2, $3, $4, $5)"
		args = append(args, b.Id)
	}

	args = append(args,
		b.Status,
		b.Data,
		b.Source,
		b.Target,
	)

	tx.MustExec(query, args...)
}

func (b *Batch) UpdateQuery(tx *sqlx.Tx) {
	tx.MustExec(
		"UPDATE teleport.batch SET status = $1 WHERE id = $2",
		b.Status,
		b.Id,
	)
}

func (b *Batch) SetEvents(events []Event) {
	// Store batch data
	var batchBuffer bytes.Buffer

	// Encode each event into buffer
	for _, event := range events {
		// Write event data to batch data
		batchBuffer.WriteString(event.String())
		batchBuffer.WriteString("\n")
	}

	// Set batch data
	dataStr := string(batchBuffer.Bytes())
	b.Data = &dataStr
}

func (b *Batch) GetEvents() []Event {
	// Split events data per line
	eventsData := strings.Split(*b.Data, "\n")

	events := make([]Event, 0)

	// Initialize new event
	for _, eventData := range eventsData {
		events = append(events, *NewEvent(eventData))
	}

	return events
}
