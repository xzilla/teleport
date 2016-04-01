package database

import (
	"fmt"
)

// Group
func (db *Database) CreateBatchesFromEvents() error {
	events, err := db.GetEvents("waiting_replication")

	if err != nil {
		return err
	}

	fmt.Printf("events: %v\n", events)

	return nil
}
