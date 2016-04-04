package database

import (
	"log"
	"time"
	"fmt"
)

type Event struct {
	Id            string `db:"id"`
	Kind          string `db:"kind"`
	Status        string `db:"status"`
	TriggerTag    string `db:"trigger_tag"`
	TriggerEvent  string `db:"trigger_event"`
	TransactionId string `db:"transaction_id"`
	BatchId       *string `db:"batch_id,omitempty"`
	Data          *string `db:"data"`
}

func (db *Database) WatchEvents(seconds time.Duration) {
	for {
		err := db.CreateBatchesFromEvents()

		if err != nil {
			log.Printf("Error creating batch! %v\n", err)
		}

		time.Sleep(seconds * time.Second)
	}
}

func (db *Database) GetEvents(status string) (*[]Event, error) {
	var events []Event
	err := db.selectObjs(&events, "SELECT * FROM teleport.event WHERE status = $1;", status)
	return &events, err
}

func (e *Event) GetUpdateQuery() string {
	return fmt.Sprintf(
		"UPDATE teleport.event SET status = '%s', data = '%s' WHERE id = %s;",
		e.Status,
		*e.Data,
		e.Id,
	)
}

// Implement Stringer 
func (e *Event) String() string {
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
