package database

import (
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/pagarme/teleport/config"
)

var db *Database

func setupEvent() {
	config := config.New()
	err := config.ReadFromFile("../config_test.yml")

	if err != nil {
		fmt.Printf("Error opening config file: %v\n", err)
		os.Exit(1)
	}

	db = New(config.Database)

	// Start db
	if err = db.Start(); err != nil {
		fmt.Printf("Erro starting database: ", err)
		os.Exit(1)
	}
}

func TestNewEvent(t *testing.T) {
	setupEvent()
	event := NewEvent("a,b,c,d,e,f")

	data := "f"

	testEvent := &Event{
		Id:            "a",
		Kind:          "b",
		Status:        "",
		TriggerTag:    "c",
		TriggerEvent:  "d",
		TransactionId: "e",
		Data:          &data,
	}

	if !reflect.DeepEqual(event, testEvent) {
		t.Errorf(
			"new event => %#v, want %#v",
			event,
			testEvent,
		)
	}
}

func TestGetEvents(t *testing.T) {
	setupEvent()
	db.Db.Exec(`
		TRUNCATE teleport.event;
		INSERT INTO teleport.event
			(id, kind, status, trigger_tag, trigger_event, transaction_id, data)
			VALUES
			(1, 'ddl', 'waiting_batch', '123', 'event', '456', 'asd');
		INSERT INTO teleport.event
			(id, kind, status, trigger_tag, trigger_event, transaction_id, data)
			VALUES
			(2, 'ddl', 'building', '123', 'event', '456', 'asd');
	`)

	data := "asd"

	testEvent := &Event{
		Id:            "1",
		Kind:          "ddl",
		Status:        "waiting_batch",
		TriggerTag:    "123",
		TriggerEvent:  "event",
		TransactionId: "456",
		Data:          &data,
	}

	events, err := db.GetEvents(nil, "waiting_batch", -1)

	if err != nil {
		t.Errorf("get events returned error: %v\n", err)
	}

	if len(events) != 1 {
		t.Errorf("get events => %d, want %d", len(events), 1)
	}

	if !reflect.DeepEqual(events[0], testEvent) {
		t.Errorf(
			"get events => %#v, want %#v",
			events[0],
			testEvent,
		)
	}
}

func TestGetEvent(t *testing.T) {
	setupEvent()
	db.Db.Exec(`
		TRUNCATE teleport.event;
		INSERT INTO teleport.event
			(id, kind, status, trigger_tag, trigger_event, transaction_id, data)
			VALUES
			(1, 'ddl', 'waiting_batch', '123', 'event', '456', 'asd_one');
		INSERT INTO teleport.event
			(id, kind, status, trigger_tag, trigger_event, transaction_id, data)
			VALUES
			(2, 'ddl', 'building', '123', 'event', '456', 'asd');
	`)

	data := "asd_one"

	testEvent := &Event{
		Id:            "1",
		Kind:          "ddl",
		Status:        "waiting_batch",
		TriggerTag:    "123",
		TriggerEvent:  "event",
		TransactionId: "456",
		Data:          &data,
	}

	event, err := db.GetEvent("1")

	if err != nil {
		t.Errorf("get event returned error: %v\n", err)
	}

	if !reflect.DeepEqual(event, testEvent) {
		t.Errorf(
			"get event => %#v, want %#v",
			event,
			testEvent,
		)
	}
}

func TestEventInsertQuery(t *testing.T) {
	setupEvent()
	db.Db.Exec(`
		TRUNCATE teleport.event;
	`)

	data := "asd_one"

	testEvent := &Event{
		Id:            "5",
		Kind:          "ddl",
		Status:        "waiting_batch",
		TriggerTag:    "123",
		TriggerEvent:  "event",
		TransactionId: "456",
		Data:          &data,
	}

	tx := db.NewTransaction()
	testEvent.InsertQuery(tx)
	tx.Commit()

	event, _ := db.GetEvent("5")

	if !reflect.DeepEqual(event, testEvent) {
		t.Errorf(
			"get event => %#v, want %#v",
			event,
			testEvent,
		)
	}
}

func TestEventUpdateQuery(t *testing.T) {
	setupEvent()
	db.Db.Exec(`
		TRUNCATE teleport.event;
	`)

	data := "asd_one"

	testEvent := &Event{
		Id:            "5",
		Kind:          "ddl",
		Status:        "waiting_batch",
		TriggerTag:    "123",
		TriggerEvent:  "event",
		TransactionId: "456",
		Data:          &data,
	}

	tx := db.NewTransaction()
	testEvent.InsertQuery(tx)
	tx.Commit()

	tx = db.NewTransaction()
	testEvent.Status = "batched"
	testEvent.UpdateQuery(tx)
	tx.Commit()

	event, _ := db.GetEvent("5")

	if !reflect.DeepEqual(event, testEvent) {
		t.Errorf(
			"get event => %#v, want %#v",
			event,
			testEvent,
		)
	}
}
