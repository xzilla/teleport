package applier

import (
	"encoding/gob"
	"fmt"
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/client"
	"github.com/pagarme/teleport/config"
	"github.com/pagarme/teleport/database"
	"os"
	"testing"
)

var db *database.Database
var stubBatch *database.Batch
var applier *Applier

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

	stubBatch = &database.Batch{
		Id:          "",
		Status:      "waiting_apply",
		DataStatus:  "waiting_apply",
		Source:      "source",
		Target:      "target",
		Data:        nil,
		StorageType: "db",
	}

	targets := make(map[string]*client.Client)

	for key, target := range config.Targets {
		targets[key] = client.New(target)
	}

	applier = New(db, 2)
}

// StubAction implements Action
type StubAction struct{
	Success bool
}

func (a *StubAction) Execute(c *action.Context) error {
	if a.Success {
		_, err := c.Tx.Exec("CREATE TABLE IF NOT EXISTS test (id SERIAL PRIMARY KEY, content TEXT); INSERT INTO test (content) VALUES ('asd');")
		return err
	} else {
		_, err := c.Tx.Exec("asdasdasd;")
		return err
	}

	return nil
}

func (a *StubAction) Filter(targetExpression string) bool {
	return true
}

func (a *StubAction) NeedsSeparatedBatch() bool {
	return false
}

func TestApplyBatchDb(t *testing.T) {
	db.Db.Exec(`
		DROP TABLE test;
		TRUNCATE teleport.batch;
	`)

	tx := db.NewTransaction()
	stubBatch = database.NewBatch("db")
	stubBatch.DataStatus = "waiting_apply"
	stubBatch.SetActions([]action.Action{&StubAction{true}})
	stubBatch.InsertQuery(tx)
	tx.Commit()

	shouldContinue, err := applier.applyBatch(stubBatch)

	if err != nil {
		t.Errorf("applyBatch returned error: %v", err)
	}

	if !shouldContinue {
		t.Errorf("shouldContinue => false, want true")
	}

	var testCount int
	db.Db.Get(&testCount, "SELECT id FROM test;")

	if testCount != 1 {
		t.Errorf("test id => %d, want %d", testCount, 1)
	}

	batches, _ := db.GetBatches("applied", "applied")

	if len(batches) != 1 {
		t.Errorf("applied batches => %d, want %d", len(batches), 1)
	}
}

func TestApplyBatchFs(t *testing.T) {
	db.Db.Exec(`
		DROP TABLE test;
		TRUNCATE teleport.batch;
	`)

	tx := db.NewTransaction()
	stubBatch = database.NewBatch("fs")
	stubBatch.DataStatus = "transmitted"
	stubBatch.SetActions([]action.Action{&StubAction{true}})
	stubBatch.InsertQuery(tx)
	tx.Commit()

	shouldContinue, err := applier.applyBatch(stubBatch)

	if err != nil {
		t.Errorf("applyBatch returned error: %v", err)
	}

	if !shouldContinue {
		t.Errorf("shouldContinue => false, want true")
	}

	var testCount int
	db.Db.Get(&testCount, "SELECT count(*) FROM test;")

	if testCount != 1 {
		t.Errorf("test id => %d, want %d", testCount, 1)
	}

	batches, _ := db.GetBatches("applied", "applied")

	if len(batches) != 1 {
		t.Errorf("applied batches => %d, want %d", len(batches), 1)
	}

	stubBatch.PurgeData()
}

func TestApplyBatchFsMultipleApplies(t *testing.T) {
	db.Db.Exec(`
		DROP TABLE test;
		TRUNCATE teleport.batch;
	`)

	tx := db.NewTransaction()
	stubBatch = database.NewBatch("fs")
	stubBatch.DataStatus = "transmitted"
	stubBatch.SetActions([]action.Action{&StubAction{true},&StubAction{true},&StubAction{true},&StubAction{true},&StubAction{true}})
	stubBatch.InsertQuery(tx)
	tx.Commit()

	shouldContinue, err := applier.applyBatch(stubBatch)

	if err != nil {
		t.Errorf("applyBatch returned error: %v", err)
	}

	if !shouldContinue {
		t.Errorf("shouldContinue => false, want true")
	}

	var testCount int
	db.Db.Get(&testCount, "SELECT count(*) FROM test;")

	if testCount != 5 {
		t.Errorf("test id => %d, want %d", testCount, 5)
	}

	batches, _ := db.GetBatches("applied", "applied")

	if len(batches) != 1 {
		t.Errorf("applied batches => %d, want %d", len(batches), 1)
	}

	stubBatch.PurgeData()
}

func TestApplyFailedBatch(t *testing.T) {
	db.Db.Exec(`
		DROP TABLE test;
		TRUNCATE teleport.batch;
	`)

	tx := db.NewTransaction()
	stubBatch = database.NewBatch("db")
	stubBatch.DataStatus = "waiting_apply"
	stubBatch.SetActions([]action.Action{&StubAction{false}, &StubAction{true}})
	stubBatch.InsertQuery(tx)
	tx.Commit()

	shouldContinue, err := applier.applyBatch(stubBatch)

	if err == nil {
		t.Errorf("applyBatch did not return error")
	}

	if !shouldContinue {
		t.Errorf("shouldContinue => false, want true")
	}

	var testCount int
	db.Db.Get(&testCount, "SELECT count(*) FROM test;")

	if testCount == 1 {
		t.Errorf("test id => %d, want 0", testCount, 0)
	}

	batches, _ := db.GetBatches("waiting_apply", "")

	if len(batches) != 1 {
		t.Errorf("waiting apply batches => %d, want %d", len(batches), 1)
	}

	if !batches[0].WaitingReexecution {
		t.Errorf("waiting apply batch is not waiting reexecution!")
	}
}
