package database

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/pagarme/teleport/action"
	"strings"
)

type Event struct {
	Id            string  `db:"id"`
	Kind          string  `db:"kind"`
	Status        string  `db:"status"`
	TriggerTag    string  `db:"trigger_tag"`
	TriggerEvent  string  `db:"trigger_event"`
	TransactionId string  `db:"transaction_id"`
	Data          *string `db:"data"`
}

type Events []Event

func NewEvent(eventData string) *Event {
	separator := strings.Split(eventData, ",")

	return &Event{
		Id:            separator[0],
		Kind:          separator[1],
		Status:        "",
		TriggerTag:    separator[2],
		TriggerEvent:  separator[3],
		TransactionId: separator[4],
		Data:          &separator[5],
	}
}

func (db *Database) GetEvents(status string) ([]*Event, error) {
	var events []*Event
	err := db.selectObjs(&events, "SELECT * FROM teleport.event WHERE status = $1 ORDER BY id ASC;", status)
	return events, err
}

func (db *Database) GetEvent(id string) (*Event, error) {
	var events []*Event
	err := db.selectObjs(&events, "SELECT * FROM teleport.event WHERE id = $1;", id)
	return events[0], err
}

func (e *Event) InsertQuery(tx *sqlx.Tx) error {
	args := make([]interface{}, 0)
	var query string

	// If there's no id, insert without id
	if e.Id == "" {
		query = "INSERT INTO teleport.event (kind, status, trigger_tag, trigger_event, transaction_id, data) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id;"
	} else {
		query = "INSERT INTO teleport.event (id, kind, status, trigger_tag, trigger_event, transaction_id, data) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id;"
		args = append(args, e.Id)
	}

	args = append(args,
		e.Kind,
		e.Status,
		e.TriggerTag,
		e.TriggerEvent,
		e.TransactionId,
		e.Data,
	)

	return tx.Get(&e.Id, query, args...)
}

func (e *Event) UpdateQuery(tx *sqlx.Tx) error {
	_, err := tx.Exec(
		"UPDATE teleport.event SET status = $1 WHERE id = $2;",
		e.Status,
		e.Id,
	)

	return err
}

func (e *Event) BelongsToBatch(tx *sqlx.Tx, b *Batch) error {
	var hasId string
	tx.Get(&hasId, "SELECT event_id FROM teleport.batch_events WHERE batch_id = $1 AND event_id = $2;",
		b.Id,
		e.Id,
	)

	if hasId == e.Id {
		return nil
	}

	_, err := tx.Exec(
		"INSERT INTO teleport.batch_events (batch_id, event_id) VALUES ($1, $2);",
		b.Id,
		e.Id,
	)

	return err
}

func (e *Event) GetBatches(db *Database) ([]Batch, error) {
	var batches []Batch

	err := db.selectObjs(&batches, `
		SELECT
			b.*
		FROM teleport.batch b
		INNER JOIN teleport.batch_events be
			ON b.id = be.batch_id
		WHERE be.event_id = $1 ORDER BY id ASC;
	`, e.Id)

	return batches, err
}

func (e *Event) SetDataFromAction(action action.Action) error {
	// Encode action into event data using gob
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(&action)

	if err != nil {
		return err
	}

	// Update event data
	encodedData := base64.StdEncoding.EncodeToString(buf.Bytes())
	e.Data = &encodedData

	return nil
}

func (e *Event) GetActionFromData() (action.Action, error) {
	decodedData, err := base64.StdEncoding.DecodeString(*e.Data)

	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	buf.Write(decodedData)

	decoder := gob.NewDecoder(&buf)
	var action action.Action
	err = decoder.Decode(&action)

	return action, err
}

// Implement ToString
func (e *Event) ToString() string {
	return fmt.Sprintf(
		"%s,%s,%s,%s,%s,%s",
		e.Id,
		e.Kind,
		e.TriggerTag,
		e.TriggerEvent,
		e.TransactionId,
		*e.Data,
	)
}

// Implement Interface
func (slice Events) Len() int {
	return len(slice)
}

func (slice Events) Less(i, j int) bool {
	return slice[i].Id < slice[j].Id
}

func (slice Events) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}
