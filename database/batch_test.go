package database

import (
	"fmt"
	"github.com/pagarme/teleport/config"
	"github.com/pagarme/teleport/action"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

var createSchemaAction *action.CreateSchema
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

	createSchemaAction = &action.CreateSchema{
		SchemaName: "test_schema",
	}

	stubBatchData = "QBAAFCphY3Rpb24uQ3JlYXRlU2NoZW1h/4EDAQEMQ3JlYXRlU2NoZW1hAf+CAAEBAQpTY2hlbWFOYW1lAQwAAAAR/4IOAQt0ZXN0X3NjaGVtYQA="

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

	testBatch.PurgeData()
}

func TestGetBatches(t *testing.T) {
	db.Db.Exec(`
		TRUNCATE teleport.batch;
		INSERT INTO teleport.batch
			(id, status, data_status, data, source, target, storage_type, waiting_reexecution)
			VALUES
			(1, 'waiting_transmission', 'waiting_transmission', 'data', 'source', 'target', 'db', false);
		INSERT INTO teleport.batch
			(id, status, data_status, data, source, target, storage_type, waiting_reexecution)
			VALUES
			(2, 'applied', 'applied', 'data', 'source', 'target', 'db', false);
	`)

	testData := "data"

	testBatch := &Batch{
		"1",
		"waiting_transmission",
		"waiting_transmission",
		"source",
		"target",
		&testData,
		"db",
		false,
		0,
	}

	batches, err := db.GetBatches("waiting_transmission", "")

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
		"1",
		"waiting_transmission",
		"waiting_transmission",
		"source",
		"target",
		&testData,
		"db",
		false,
		0,
	}

	tx := db.NewTransaction()
	testBatch.InsertQuery(tx)
	tx.Commit()

	batches, _ := db.GetBatches("waiting_transmission", "")

	if !reflect.DeepEqual(batches[0], testBatch) {
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
		"1",
		"waiting_transmission",
		"waiting_transmission",
		"source",
		"target",
		&testData,
		"db",
		false,
		0,
	}

	tx := db.NewTransaction()
	testBatch.InsertQuery(tx)
	tx.Commit()

	tx = db.NewTransaction()
	testBatch.Status = "applied"
	testBatch.UpdateQuery(tx)
	tx.Commit()

	batches, _ := db.GetBatches("applied", "")

	if !reflect.DeepEqual(batches[0], testBatch) {
		t.Errorf(
			"get batch => %#v, want %#v",
			batches[0],
			testBatch,
		)
	}

	batch, _ := db.GetBatch(batches[0].Id)

	if !reflect.DeepEqual(batches[0], batch) {
		t.Errorf(
			"get batch => %#v, want %#v",
			batch,
			batches[0],
		)
	}
}

func TestBatchSetActions(t *testing.T) {
	testBatch := &Batch{
		"1",
		"waiting_transmission",
		"waiting_transmission",
		"source",
		"target",
		nil,
		"db",
		false,
		0,
	}

	testBatch.SetActions([]action.Action{createSchemaAction})

	if *testBatch.Data != stubBatchData {
		t.Errorf("batch data => %#v, want %#v", testBatch.Data, stubBatchData)
	}
}

func TestBatchGetActions(t *testing.T) {
	testBatch := &Batch{
		"1",
		"waiting_transmission",
		"waiting_transmission",
		"source",
		"target",
		&stubBatchData,
		"db",
		false,
		0,
	}

	actions, _ := testBatch.GetActions()

	if !reflect.DeepEqual(actions[0], createSchemaAction) {
		t.Errorf(
			"get actions => %#v, want %#v",
			actions[0],
			createSchemaAction,
		)
	}
}

func TestBatchAppendActions(t *testing.T) {
	testBatch := NewBatch("db")

	err := testBatch.AppendActions([]action.Action{createSchemaAction})

	if err == nil {
		t.Errorf("append action for db storage did not return error!")
	}

	testBatch = NewBatch("fs")

	err = testBatch.AppendActions([]action.Action{createSchemaAction})
	err = testBatch.AppendActions([]action.Action{createSchemaAction})

	if err != nil {
		t.Errorf("append action returned error: %#v\n", err)
	}

	actions, _ := testBatch.GetActions()
	output := []action.Action{createSchemaAction, createSchemaAction}

	if len(actions) != 2 {
		t.Errorf("get actions => %d, want %d", len(actions), 2)
	}

	if !reflect.DeepEqual(actions, output) {
		t.Errorf(
			"get actions after append => %#v, want %#v",
			actions,
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
