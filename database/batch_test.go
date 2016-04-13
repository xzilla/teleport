package database

import (
	"fmt"
	"github.com/pagarme/teleport/config"
	"os"
	"reflect"
	"testing"
)

var stubEvent *Event
var stubBatchData string

func init() {
	config := config.New()
	err := config.ReadFromFile("../config_test.yml")

	if err != nil {
		fmt.Printf("Error opening config file: %v\n", err)
		os.Exit(1)
	}

	db = New(
		config.Database.Name,
		config.Database.Database,
		config.Database.Hostname,
		config.Database.Username,
		config.Database.Password,
		config.Database.Port,
	)

	data := "f1"

	stubEvent = &Event{
		Id:            "a1",
		Kind:          "b1",
		Status:        "",
		TriggerTag:    "c1",
		TriggerEvent:  "d1",
		TransactionId: "e1",
		Data:          &data,
	}

	stubBatchData = "a1,b1,c1,d1,e1,f1"

	// Start db
	if err = db.Start(); err != nil {
		fmt.Printf("Erro starting database: ", err)
		os.Exit(1)
	}
}

func TestGetBatches(t *testing.T) {
	db.db.Exec(`
		TRUNCATE teleport.batch;
		INSERT INTO teleport.batch
			(id, status, data, source, target)
			VALUES
			(1, 'waiting_transmission', 'data', 'source', 'target');
		INSERT INTO teleport.batch
			(id, status, data, source, target)
			VALUES
			(2, 'applied', 'data 2', 'source', 'target');
	`)

	testData := "data"

	testBatch := Batch{
		Id: "1",
		Status: "waiting_transmission",
		Source: "source",
		Target: "target",
		Data: &testData,
	}

	batches, err := db.GetBatches("waiting_transmission")

	if err != nil {
		t.Errorf("get batches returned error: %v\n", err)
	}

	if len(batches) != 1 {
		t.Errorf("get batches => %d, want %d", len(batches), 1)
	}

	if !reflect.DeepEqual(batches[0], testBatch) {
		t.Errorf(
			"get batches => %#v, want %#v",
			batches[0],
			testBatch,
		)
	}
}

func TestBatchInsertQuery(t *testing.T) {
	db.db.Exec(`
		TRUNCATE teleport.batch;
	`)

	testData := "data"

	testBatch := &Batch{
		Id: "1",
		Status: "waiting_transmission",
		Source: "source",
		Target: "target",
		Data: &testData,
	}

	tx := db.NewTransaction()
	testBatch.InsertQuery(tx)
	tx.Commit()

	batches, _ := db.GetBatches("waiting_transmission")

	if !reflect.DeepEqual(batches[0], *testBatch) {
		t.Errorf(
			"get batch => %#v, want %#v",
			batches[0],
			testBatch,
		)
	}
}

func TestBatchUpdateQuery(t *testing.T) {
	db.db.Exec(`
		TRUNCATE teleport.batch;
	`)

	testData := "data"

	testBatch := &Batch{
		Id: "1",
		Status: "waiting_transmission",
		Source: "source",
		Target: "target",
		Data: &testData,
	}

	tx := db.NewTransaction()
	testBatch.InsertQuery(tx)
	tx.Commit()

	tx = db.NewTransaction()
	testBatch.Status = "applied"
	testBatch.UpdateQuery(tx)
	tx.Commit()

	batches, _ := db.GetBatches("applied")

	if !reflect.DeepEqual(batches[0], *testBatch) {
		t.Errorf(
			"get batch => %#v, want %#v",
			batches[0],
			testBatch,
		)
	}
}

func TestBatchSetEvents(t *testing.T) {
	testBatch := &Batch{
		Id: "1",
		Status: "waiting_transmission",
		Source: "source",
		Target: "target",
		Data: nil,
	}

	testBatch.SetEvents([]Event{*stubEvent})

	if *testBatch.Data != stubBatchData {
		t.Errorf("batch data => %#v, want %#v", testBatch.Data, stubBatchData)
	}
}

func TestBatchGetEvents(t *testing.T) {
	testBatch := &Batch{
		Id: "1",
		Status: "waiting_transmission",
		Source: "source",
		Target: "target",
		Data: &stubBatchData,
	}

	events := testBatch.GetEvents()

	if !reflect.DeepEqual(events[0], *stubEvent) {
		t.Errorf(
			"get events => %#v, want %#v",
			events[0],
			testBatch,
		)
	}
}
