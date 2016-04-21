package database

import (
	"bytes"
	"fmt"
	"github.com/jmoiron/sqlx"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"
)

type Batch struct {
	Id          string  `db:"id" json:"id"`
	Status      string  `db:"status" json:"status"`
	Source      string  `db:"source" json:"source"`
	Target      string  `db:"target" json:"target"`
	Data        *string `db:"data" json:"data"`
	StorageType string  `db:"storage_type" json:"storage_type"`
}

func NewBatch(storageType string) *Batch {
	batch := &Batch{
		Status:      "waiting_transmission",
		StorageType: storageType,
	}

	if batch.StorageType == "fs" {
		batch.generateBatchFilename()
		batch.appendData(nil)
	}

	return batch
}

func (db *Database) GetBatches(status string) ([]Batch, error) {
	var batches []Batch
	err := db.selectObjs(&batches, "SELECT * FROM teleport.batch WHERE status = $1 ORDER BY id ASC;", status)
	return batches, err
}

func (b *Batch) generateBatchFilename() {
	if b.StorageType != "fs" {
		panic("batch storage type is not fs")
	}

	rand.Seed(time.Now().UTC().UnixNano())
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, 20)
	for i := 0; i < 20; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}
	filename := string(result)
	b.Data = &filename
}

func (b *Batch) appendData(data *string) error {
	if b.StorageType != "fs" {
		return fmt.Errorf("appending data is only supported in fs storage type")
	}

	f, err := os.OpenFile(*b.Data, os.O_APPEND|os.O_WRONLY, 0600)

	if err != nil {
		return err
	}

	defer f.Close()

	_, err = f.WriteString(*data)

	if err != nil {
		return err
	}

	return nil
}

func (b *Batch) setData(data *string) error {
	if b.StorageType == "fs" {
		f, err := os.OpenFile(*b.Data, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)

		if err != nil {
			return err
		}

		defer f.Close()

		_, err = f.WriteString(*data)

		if err != nil {
			return err
		}
	} else {
		b.Data = data
	}

	return nil
}

func (b *Batch) getData() (*string, error) {
	if b.StorageType == "fs" {
		data, err := ioutil.ReadFile(*b.Data)

		if err != nil {
			return nil, err
		}

		content := string(data)
		return &content, nil
	} else {
		return b.Data, nil
	}

	return nil, nil
}

func (b *Batch) InsertQuery(tx *sqlx.Tx) error {
	args := make([]interface{}, 0)
	var query string

	// If there's no id, insert without id
	if b.Id == "" {
		query = "INSERT INTO teleport.batch (status, data, source, target, storage_type) VALUES ($1, $2, $3, $4, $5) RETURNING id;"
	} else {
		query = "INSERT INTO teleport.batch (id, status, data, source, target, storage_type) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id;"
		args = append(args, b.Id)
	}

	args = append(args,
		b.Status,
		b.Data,
		b.Source,
		b.Target,
		b.StorageType,
	)

	return tx.Get(&b.Id, query, args...)
}

func (b *Batch) UpdateQuery(tx *sqlx.Tx) error {
	_, err := tx.Exec(
		"UPDATE teleport.batch SET status = $1 WHERE id = $2",
		b.Status,
		b.Id,
	)

	return err
}

func (b *Batch) SetEvents(events Events) error {
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
	return b.setData(&dataStr)
}

func (b *Batch) GetEvents() (Events, error) {
	// Split events data per line
	events := make(Events, 0)

	if *b.Data == "" {
		return events, nil
	}

	data, err := b.getData()

	if err != nil {
		return events, err
	}

	eventsData := strings.Split(*data, "\n")

	// Initialize new event
	for _, eventData := range eventsData {
		events = append(events, *NewEvent(eventData))
	}

	// Sort events by id before returning
	sort.Sort(events)

	return events, nil
}

func (b *Batch) AppendEvents(tx *sqlx.Tx, events Events) error {
	existingEvents, err := b.GetEvents()

	if err != nil {
		return err
	}

	for _, newEvent := range events {
		existingEvents = append(existingEvents, newEvent)
	}

	b.SetEvents(existingEvents)
	b.UpdateQuery(tx)

	return nil
}
