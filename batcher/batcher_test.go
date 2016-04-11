package batcher

import (
	"testing"
	"github.com/pagarme/teleport/config"
	"github.com/pagarme/teleport/database"
	"github.com/pagarme/teleport/client"
	"github.com/pagarme/teleport/action"
	"github.com/jmoiron/sqlx"
	"encoding/gob"
	"os"
	"fmt"
)

var db *database.Database
var stubEvent *database.Event
var batcher *Batcher

func init() {
	gob.Register(&StubAction{})

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

// StubAction implements Action
type StubAction struct {
	ShouldFilter bool
}

func (a *StubAction) Execute(tx *sqlx.Tx) {
}

func (a *StubAction) Filter(targetExpression string) bool {
	return a.ShouldFilter
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

func TestEventsForTarget(t *testing.T) {
	events, _ := batcher.eventsForTarget(
		batcher.targets["test_target"],
		map[database.Event][]action.Action{
			*stubEvent: []action.Action{&StubAction{false}},
		},
	)

	if len(events) != 0 {
		t.Errorf("events for target => %d, want %d", len(events), 0)
	}

	events, _ = batcher.eventsForTarget(
		batcher.targets["test_target"],
		map[database.Event][]action.Action{
			*stubEvent: []action.Action{&StubAction{true}},
		},
	)

	if len(events) != 1 {
		t.Errorf("events for target => %d, want %d", len(events), 1)
	}
}

func TestCreateBatchWithEvents(t *testing.T) {
	stubEventData := "ASD"
	stubEvent.Data = &stubEventData

	batch, err := batcher.createBatchWithEvents([]database.Event{*stubEvent}, "test-target")

	if err != nil {
		t.Errorf("createBatchWithEvents returned error: %v", err)
	}

	if batch.Source != "test-db" {
		t.Errorf("batch source => %s, want %", batch.Source, "test-db")
	}

	if batch.Target != "test-target" {
		t.Errorf("batch source => %s, want %", batch.Target, "test-target")
	}

	if batch.Status != "waiting_transmission" {
		t.Errorf("batch status => %s, want %", batch.Status, "waiting_transmission")
	}

	event, _ := db.GetEvent(stubEvent.Id)

	if event.Status != "batched" {
		t.Errorf("event status => %s, want %s", event.Status, "batched")
	}
}
