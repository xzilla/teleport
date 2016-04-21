package database

import (
	"bytes"
	"github.com/jmoiron/sqlx"
	"sort"
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

func (b *Batch) InsertQuery(tx *sqlx.Tx) error {
	args := make([]interface{}, 0)
	var query string

	// If there's no id, insert without id
	if b.Id == "" {
		query = "INSERT INTO teleport.batch (status, data, source, target) VALUES ($1, $2, $3, $4) RETURNING id;"
	} else {
		query = "INSERT INTO teleport.batch (id, status, data, source, target) VALUES ($1, $2, $3, $4, $5) RETURNING id;"
		args = append(args, b.Id)
	}

	args = append(args,
		b.Status,
		b.Data,
		b.Source,
		b.Target,
	)

	return tx.Get(&b.Id, query, args...)
}

func (b *Batch) UpdateQuery(tx *sqlx.Tx) error {
	_, err := tx.Exec(
		"UPDATE teleport.batch SET status = $1, data = $2 WHERE id = $3",
		b.Status,
		b.Data,
		b.Id,
	)

	return err
}

func (b *Batch) SetEvents(events Events) {
	// Store batch data
	var batchBuffer bytes.Buffer

	// Sort events by id first
	sort.Sort(events)

	// Encode each event into buffer
	for i, event := range events {
		// Write event data to batch data
		batchBuffer.WriteString(event.ToString())

		// Don't write newline after the last event
		if i < len(events)-1 {
			batchBuffer.WriteString("\n")
		}
	}

	// Set batch data
	dataStr := string(batchBuffer.Bytes())
	b.Data = &dataStr
}

func (b *Batch) GetEvents() Events {
	// Split events data per line
	events := make(Events, 0)

	if *b.Data == "" {
		return events
	}

	eventsData := strings.Split(*b.Data, "\n")

	// Initialize new event
	for _, eventData := range eventsData {
		events = append(events, *NewEvent(eventData))
	}

	// Sort events by id before returning
	sort.Sort(events)

	return events
}

func (b *Batch) AppendEvents(tx *sqlx.Tx, events Events) {
	existingEvents := b.GetEvents()

	for _, newEvent := range events {
		existingEvents = append(existingEvents, newEvent)
	}

	b.SetEvents(existingEvents)
	b.UpdateQuery(tx)
}
