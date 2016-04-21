package database

import (
	"fmt"
	"github.com/pagarme/teleport/config"
	"io/ioutil"
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

func TestNewBatch(t *testing.T) {
	testBatch := NewBatch("db")

	if testBatch.Data != nil {
		t.Errorf("data => %#v, want nil", *testBatch.Data)
	}

	// Should initialize the batch file
	testBatch = NewBatch("fs")

	if testBatch.Data == nil {
		t.Errorf("data => nil, want not nil")
	}
}

func TestGetBatches(t *testing.T) {
	db.Db.Exec(`
		TRUNCATE teleport.batch;
		INSERT INTO teleport.batch
			(id, status, data, source, target, storage_type)
			VALUES
			(1, 'waiting_transmission', 'data', 'source', 'target', 'db');
		INSERT INTO teleport.batch
			(id, status, data, source, target)
			VALUES
			(2, 'applied', 'data 2', 'source', 'target');
	`)

	testData := "data"

	testBatch := Batch{
		Id:          "1",
		Status:      "waiting_transmission",
		Source:      "source",
		Target:      "target",
		Data:        &testData,
		StorageType: "db",
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
	db.Db.Exec(`
		TRUNCATE teleport.batch;
	`)

	testData := "data"

	testBatch := &Batch{
		Id:          "1",
		Status:      "waiting_transmission",
		Source:      "source",
		Target:      "target",
		Data:        &testData,
		StorageType: "db",
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
	db.Db.Exec(`
		TRUNCATE teleport.batch;
	`)

	testData := "data"

	testBatch := &Batch{
		Id:          "1",
		Status:      "waiting_transmission",
		Source:      "source",
		Target:      "target",
		Data:        &testData,
		StorageType: "db",
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
		Id:          "1",
		Status:      "waiting_transmission",
		Source:      "source",
		Target:      "target",
		Data:        nil,
		StorageType: "db",
	}

	testBatch.SetEvents([]Event{*stubEvent})

	if *testBatch.Data != stubBatchData {
		t.Errorf("batch data => %#v, want %#v", testBatch.Data, stubBatchData)
	}
}

func TestBatchGetEvents(t *testing.T) {
	testBatch := &Batch{
		Id:          "1",
		Status:      "waiting_transmission",
		Source:      "source",
		Target:      "target",
		Data:        &stubBatchData,
		StorageType: "db",
	}

	events, _ := testBatch.GetEvents()

	if !reflect.DeepEqual(events[0], *stubEvent) {
		t.Errorf(
			"get events => %#v, want %#v",
			events[0],
			testBatch,
		)
	}
}

func TestBatchAppendEvents(t *testing.T) {
	testBatch := NewBatch("db")

	err := testBatch.AppendEvents([]Event{*stubEvent})

	if err == nil {
		t.Errorf("append event for db storage did not return error!")
	}

	testBatch = NewBatch("fs")

	err = testBatch.AppendEvents([]Event{*stubEvent})
	err = testBatch.AppendEvents([]Event{*stubEvent})

	if err != nil {
		t.Errorf("append event returned error: %#v\n", err)
	}

	events, _ := testBatch.GetEvents()
	output := Events{*stubEvent, *stubEvent}

	if len(events) != 2 {
		t.Errorf("get events => %d, want %d", len(events), 2)
	}

	if !reflect.DeepEqual(events, output) {
		t.Errorf(
			"get events after append => %#v, want %#v",
			events,
			output,
		)
	}

	testBatch.PurgeData()
}

func TestBatchGetSetData(t *testing.T) {
	data := "asdasdasdasd"

	testBatch := NewBatch("db")
	err := testBatch.SetData(&data)

	if err != nil {
		t.Errorf("set data returned error: %#v\n", err)
	}

	resultData, err := testBatch.GetData()

	if err != nil {
		t.Errorf("get data returned error: %#v\n", err)
	}

	if *resultData != data {
		t.Errorf("result data => %#v, want %#v", *resultData, data)
	}

	testBatch = NewBatch("fs")
	err = testBatch.SetData(&data)

	if err != nil {
		t.Errorf("set data returned error: %#v\n", err)
	}

	resultData, err = testBatch.GetData()

	if err != nil {
		t.Errorf("get data returned error: %#v\n", err)
	}

	if *resultData != data {
		t.Errorf("result data => %#v, want %#v", *resultData, data)
	}

	newData := "appended data!!!!!!!!"
	err = testBatch.AppendData(&newData)

	if err != nil {
		t.Errorf("append data returned error: %#v\n", err)
	}

	resultData, err = testBatch.GetData()

	if err != nil {
		t.Errorf("get data returned error: %#v\n", err)
	}

	expectedOutput := fmt.Sprintf("%s%s", data, newData)

	if *resultData != expectedOutput {
		t.Errorf("result data => %#v, want %#v", *resultData, expectedOutput)
	}

	fileData, err := ioutil.ReadFile(*testBatch.Data)

	if err != nil {
		t.Errorf("read file returned error: %#v", err)
	}

	if string(fileData) != *resultData {
		t.Errorf("file data -> %#v, want %#v", fileData, *resultData)
	}

	oldFilename := *testBatch.Data
	err = testBatch.PurgeData()

	if err != nil {
		t.Errorf("purge data returned error: %#v", err)
	}

	_, err = ioutil.ReadFile(oldFilename)

	if err == nil {
		t.Errorf("read file did not return error for purged file!")
	}
}
