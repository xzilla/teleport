package database

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/pagarme/teleport/action"
	"io/ioutil"
	"encoding/base64"
	"encoding/gob"
	"math/rand"
	"os"
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
	WaitingReexecution bool  `db:"waiting_reexecution" json:"waiting_reexecution"`
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

func (db *Database) GetBatches(status string) ([]*Batch, error) {
	var batches []*Batch
	err := db.selectObjs(&batches, "SELECT * FROM teleport.batch WHERE status = $1 ORDER BY waiting_reexecution, id ASC;", status)
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
		"UPDATE teleport.batch SET status = $1, data = $2, waiting_reexecution = $3 WHERE id = $4",
		b.Status,
		b.Data,
		b.WaitingReexecution,
		b.Id,
	)

	return err
}

func (b *Batch) dataForActions(actions []action.Action) (string, error) {
	// Store batch data
	var batchBuffer bytes.Buffer

	// Encode each action into buffer
	for i, act := range actions {
		// Encode action using gob
		var buf bytes.Buffer
		encoder := gob.NewEncoder(&buf)
		err := encoder.Encode(&act)

		if err != nil {
			return "", err
		}

		encodedData := base64.StdEncoding.EncodeToString(buf.Bytes())

		// Write action data to batch data
		batchBuffer.WriteString(encodedData)

		// Don't write newline after the last action
		if i < len(actions)-1 {
			batchBuffer.WriteString("\n")
		}
	}

	// Return batch data
	return string(batchBuffer.Bytes()), nil
}

func (b *Batch) SetActions(actions []action.Action) error {
	data, err := b.dataForActions(actions)

	if err != nil {
		return err
	}

	return b.SetData(&data)
}

func (b *Batch) ActionFromData(data string) (action.Action, error) {
	if data == "" {
		return nil, nil
	}

	decodedData, err := base64.StdEncoding.DecodeString(data)

	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	buf.Write(decodedData)

	decoder := gob.NewDecoder(&buf)
	var action action.Action
	err = decoder.Decode(&action)
	
	return action, nil
}

func (b *Batch) GetActions() ([]action.Action, error) {
	actions := make([]action.Action, 0)

	data, err := b.GetData()

	if err != nil {
		return actions, err
	}

	if *data == "" {
		return actions, nil
	}

	// Split action data per line
	actionsData := strings.Split(*data, "\n")

	// Initialize new action
	for _, actionData := range actionsData {
		act, err := b.ActionFromData(actionData)

		if err != nil {
			return actions, err
		}

		if act != nil {
			actions = append(actions, act)
		}
	}

	return actions, nil
}

func (b *Batch) AppendActions(actions []action.Action) error {
	data, err := b.dataForActions(actions)

	if err != nil {
		return err
	}

	data = fmt.Sprintf("\n%s", data)
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

func (b *Batch) GetFileScanner() (*bufio.Scanner, *os.File, error) {
	file, err := b.GetFile()

	if err != nil {
		return nil, nil, err
	}

	scanner := bufio.NewScanner(file)

	return scanner, file, nil
}
