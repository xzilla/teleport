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
		tempData := ""
		batch.SetData(&tempData)
	}

	return batch
}

func (db *Database) GetBatches(status string) ([]Batch, error) {
	var batches []Batch
	err := db.selectObjs(&batches, "SELECT * FROM teleport.batch WHERE status = $1 ORDER BY id ASC;", status)
	return batches, err
}

func (db *Database) GetBatch(id string) (*Batch, error) {
	var batches []*Batch
	err := db.selectObjs(&batches, "SELECT * FROM teleport.batch WHERE id = $1;", id)

	if err != nil {
		return nil, err
	}

	if len(batches) == 0 {
		return nil, nil
	}

	return batches[0], nil
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
	filename := fmt.Sprintf("%s.csv", string(result))
	b.Data = &filename
}

func (b *Batch) AppendData(data *string) error {
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

func (b *Batch) SetData(data *string) error {
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

func (b *Batch) GetData() (*string, error) {
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

func (b *Batch) PurgeData() error {
	if b.StorageType == "fs" {
		err := os.Remove(*b.Data)

		if err != nil {
			return err
		}
	}

	b.Data = nil

	return nil
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
		"UPDATE teleport.batch SET status = $1, data = $2 WHERE id = $3",
		b.Status,
		b.Data,
		b.Id,
	)

	return err
}

func (b *Batch) generateDataForEvents(events Events) string {
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

	// Return batch data
	return string(batchBuffer.Bytes())
}

func (b *Batch) SetEvents(events Events) error {
	data := b.generateDataForEvents(events)
	return b.SetData(&data)
}

func (b *Batch) GetEvents() (Events, error) {
	// Split events data per line
	events := make(Events, 0)

	data, err := b.GetData()

	if err != nil {
		return events, err
	}

	if *data == "" {
		return events, nil
	}

	eventsData := strings.Split(*data, "\n")

	// Initialize new event
	for _, eventData := range eventsData {
		if eventData != "" {
			events = append(events, *NewEvent(eventData))
		}
	}

	// Sort events by id before returning
	sort.Sort(events)

	return events, nil
}

func (b *Batch) AppendEvents(events Events) error {
	data := fmt.Sprintf("\n%s", b.generateDataForEvents(events))
	return b.AppendData(&data)
}

func (b *Batch) CreateFile() (*os.File, error) {
	if b.StorageType != "fs" {
		return nil, fmt.Errorf("batch storage type is not fs")
	}

	return os.Create(*b.Data)
}

func (b *Batch) GetFile() (*os.File, error) {
	if b.StorageType != "fs" {
		return nil, fmt.Errorf("batch storage type is not fs")
	}

	return os.Open(*b.Data)
}
