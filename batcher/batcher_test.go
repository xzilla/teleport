package batcher

import (
	"encoding/gob"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/client"
	"github.com/pagarme/teleport/config"
	"github.com/pagarme/teleport/database"
	"os"
	"testing"
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
		Id:            "",
		Kind:          "ddl",
		Status:        "waiting_batch",
		TriggerTag:    "TAG",
		TriggerEvent:  "EVENT",
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

func (a *StubAction) Execute(tx *sqlx.Tx) error {
	return nil
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
	tx := db.NewTransaction()
	stubEventData := "event data"
	stubEvent.Data = &stubEventData
	stubEvent.InsertQuery(tx)
	tx.Commit()

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
	tx := db.NewTransaction()
	stubEventData := "event data"
	stubEvent.Data = &stubEventData
	stubEvent.InsertQuery(tx)
	tx.Commit()

	batch, err := batcher.createBatchWithEvents([]database.Event{*stubEvent}, "test-target")

	if err != nil {
		t.Errorf("createBatchWithEvents returned error: %v", err)
	}

	if batch.Source != "test-db" {
		t.Errorf("batch source => %s, want %s", batch.Source, "test-db")
	}

	if batch.Target != "test-target" {
		t.Errorf("batch source => %s, want %s", batch.Target, "test-target")
	}

	if batch.Status != "waiting_transmission" {
		t.Errorf("batch status => %s, want %s", batch.Status, "waiting_transmission")
	}

	event, _ := db.GetEvent(stubEvent.Id)

	if event.Status != "batched" {
		t.Errorf("event status => %s, want %s", event.Status, "batched")
	}

	tx = db.NewTransaction()
	var batchId string
	tx.Get(&batchId, "SELECT batch_id FROM teleport.batch_events WHERE batch_id = $1 AND event_id = $2;", batch.Id, stubEvent.Id)

	if batchId != batch.Id {
		t.Errorf("batch_id in batch_events table => %s, want %s", batchId, batch.Id)
	}
}
