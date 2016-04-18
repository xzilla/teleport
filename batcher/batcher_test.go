package batcher

import (
	"encoding/gob"
	"fmt"
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/client"
	"github.com/pagarme/teleport/config"
	"github.com/pagarme/teleport/database"
	"os"
	"reflect"
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

	db = database.New(
		config.Database.Name,
		config.Database.Database,
		config.Database.Hostname,
		config.Database.Username,
		config.Database.Password,
		config.Database.Port,
	)

	// Start db
	if err = db.Start(); err != nil {
		fmt.Printf("Erro starting database: ", err)
		os.Exit(1)
	}

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
	ShouldFilter  bool
	SeparateBatch bool
}

func (a *StubAction) Execute(c action.Context) error {
	return nil
}

func (a *StubAction) Filter(targetExpression string) bool {
	return a.ShouldFilter
}

func (a *StubAction) NeedsSeparatedBatch() bool {
	return a.SeparateBatch
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

func TestActionsForEvents(t *testing.T) {
	testAction := &StubAction{true, false}

	tx := db.NewTransaction()
	stubEvent.Kind = "test"
	stubEvent.SetDataFromAction(testAction)
	stubEvent.InsertQuery(tx)
	tx.Commit()

	output := map[database.Event][]action.Action{
		*stubEvent: []action.Action{
			testAction,
		},
	}

	actionsForEvents, _ := batcher.actionsForEvents([]database.Event{*stubEvent})

	if !reflect.DeepEqual(actionsForEvents, output) {
		t.Errorf(
			"action for event => %#v, want %#v",
			actionsForEvents,
			output,
		)
	}
}

func TestCreateBatchWithEvents(t *testing.T) {
	tx := db.NewTransaction()
	stubEventData := "event data"
	stubEvent.Data = &stubEventData
	stubEvent.InsertQuery(tx)
	tx.Commit()

	batch, err := batcher.createBatchWithEvents([]database.Event{*stubEvent}, "test_target")

	if err != nil {
		t.Errorf("createBatchWithEvents returned error: %v", err)
	}

	if batch.Source != "test-db" {
		t.Errorf("batch source => %s, want %s", batch.Source, "test-db")
	}

	if batch.Target != "test_target" {
		t.Errorf("batch source => %s, want %s", batch.Target, "test_target")
	}

	if batch.Status != "waiting_transmission" {
		t.Errorf("batch status => %s, want %s", batch.Status, "waiting_transmission")
	}

	event, _ := db.GetEvent(stubEvent.Id)

	if event.Status != "batched" {
		t.Errorf("event status => %s, want %s", event.Status, "batched")
	}

	var batchId string
	db.Db.Get(&batchId, "SELECT batch_id FROM teleport.batch_events WHERE batch_id = $1 AND event_id = $2;", batch.Id, stubEvent.Id)

	if batchId != batch.Id {
		t.Errorf("batch_id in batch_events table => %s, want %s", batchId, batch.Id)
	}
}

func TestCreateBatchesWithActions(t *testing.T) {
	db.Db.Exec(`
		TRUNCATE teleport.event;
		TRUNCATE teleport.batch;
	`)

	tx := db.NewTransaction()
	stubEvent.InsertQuery(tx)
	tx.Commit()

	usedEvents, batches, err := batcher.CreateBatchesWithActions(
		map[database.Event][]action.Action{
			*stubEvent: []action.Action{
				&StubAction{true, false},
				&StubAction{true, false},
				&StubAction{true, false},
				&StubAction{true, false},
			},
		},
	)

	if err != nil {
		t.Errorf("createBatchWithEvents returned error: %v", err)
	}

	if len(usedEvents) != 4 {
		t.Errorf("usedEvents => %d, want %d", len(usedEvents), 4)
	}

	if len(batches) != 1 {
		t.Errorf("batches => %d, want %d", len(batches), 3)
	}

	if len(batches[0].GetEvents()) != 4 {
		t.Errorf("batch 0 => %d, want %d", len(batches[0].GetEvents()), 4)
	}

	usedEvents, batches, err = batcher.CreateBatchesWithActions(
		map[database.Event][]action.Action{
			*stubEvent: []action.Action{
				&StubAction{true, false},
				&StubAction{true, false},
				&StubAction{true, true},
				&StubAction{true, false},
			},
		},
	)

	if err != nil {
		t.Errorf("createBatchWithEvents returned error: %v", err)
	}

	if len(usedEvents) != 4 {
		t.Errorf("usedEvents => %d, want %d", len(usedEvents), 4)
	}

	if len(batches) != 3 {
		t.Errorf("batches => %d, want %d", len(batches), 3)
	}

	if len(batches[0].GetEvents()) != 2 {
		t.Errorf("batch 0 => %d, want %d", len(batches[0].GetEvents()), 2)
	}

	if len(batches[1].GetEvents()) != 1 {
		t.Errorf("batch 1 => %d, want %d", len(batches[1].GetEvents()), 1)
	}

	if len(batches[2].GetEvents()) != 1 {
		t.Errorf("batch 2 => %d, want %d", len(batches[2].GetEvents()), 1)
	}
}
