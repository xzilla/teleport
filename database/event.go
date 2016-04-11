package database

import (
	"fmt"
	"github.com/jmoiron/sqlx"
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

func (db *Database) GetEvents(status string) ([]Event, error) {
	var events []Event
	err := db.selectObjs(&events, "SELECT * FROM teleport.event WHERE status = $1 ORDER BY id ASC;", status)
	return events, err
}

func (e *Event) InsertQuery(tx *sqlx.Tx) error {
	fmt.Printf("inserting event: %v\n", e)

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

	rows := tx.QueryRowx(query, args...)
	rows.Scan(&e.Id)

	return rows.Err()
}

func (e *Event) UpdateQuery(tx *sqlx.Tx) error {
	return tx.QueryRowx(
		"UPDATE teleport.event SET status = $1 WHERE id = $2;",
		e.Status,
		e.Id,
	).Err()
}

func (e *Event) BelongsToBatch(tx *sqlx.Tx, b *Batch) error {
	return tx.QueryRowx(
		"INSERT INTO teleport.batch_events (batch_id, event_id) VALUES ($1, $2);",
		b.Id,
		e.Id,
	).Err()
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
