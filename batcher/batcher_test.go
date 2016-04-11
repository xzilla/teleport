package batcher

import (
	"testing"
	"github.com/pagarme/teleport/config"
	"github.com/pagarme/teleport/database"
	"github.com/pagarme/teleport/client"
	"github.com/pagarme/teleport/action"
	"os"
	"fmt"
)

var db *database.Database
var stubEvent *database.Event
var batcher *Batcher

func init() {
	config := config.New()
	err := config.ReadFromFile("../config_test.yml")

	if err != nil {
		fmt.Printf("Error opening config file: %v\n", err)
		os.Exit(1)
	}

	// Start db
	if err = config.Database.Start(); err != nil {
		fmt.Printf("Erro starting database: ", err)
		os.Exit(1)
	}

	db = &config.Database

	stubEvent = &database.Event{
		Id: "",
		Kind: "ddl",
		Status: "waiting_batch",
		TriggerTag: "TAG",
		TriggerEvent: "EVENT",
		TransactionId: "123",
	}

	targets := make(map[string]*client.Client)

	for key, target := range config.Targets {
		targets[key] = client.New(target)
	}

	batcher = New(db, targets)
}

func TestMarkIgnoredEvents(t *testing.T) {
	tx := db.NewTransaction()
	stubEvent.InsertQuery(tx)
	tx.Commit()

	batcher.markIgnoredEvents([]database.Event{}, map[database.Event][]action.Action{
		*stubEvent: []action.Action{},
	})

	ignoredEvents, _ := db.GetEvents("ignored")
	var updatedEvent *database.Event

	for _, event := range ignoredEvents {
		if stubEvent.Id == event.Id {
			updatedEvent = &event
			break
		}
	}

	if updatedEvent == nil {
		t.Errorf("ignored event => nil, want %d", stubEvent)
	}
}
