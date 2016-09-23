package database

import (
	"github.com/jmoiron/sqlx"
	"strconv"
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

type Events []*Event

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

func (db *Database) GetEvents(tx *sqlx.Tx, status string, limit int) ([]*Event, error) {
	var err error
	var events []*Event

	if limit <= 0 {
		err = db.selectObjs(tx, &events, "SELECT * FROM teleport.event WHERE status = $1 ORDER BY id ASC;", status)
	} else {
		err = db.selectObjs(tx, &events, "SELECT * FROM teleport.event WHERE status = $1 ORDER BY id ASC LIMIT $2;", status, limit)
	}

	return events, err
}

func (db *Database) GetEvent(id string) (*Event, error) {
	var events []*Event
	err := db.selectObjs(nil, &events, "SELECT * FROM teleport.event WHERE id = $1;", id)
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

// Implement Interface
func (slice Events) Len() int {
	return len(slice)
}

func (slice Events) Less(i, j int) bool {
	iInt, _ := strconv.Atoi(slice[i].Id)
	jInt, _ := strconv.Atoi(slice[j].Id)
	return iInt < jInt
}

func (slice Events) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}
