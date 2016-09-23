package batcher

import (
	"encoding/gob"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/kylelemons/godebug/pretty"
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/client"
	"github.com/pagarme/teleport/config"
	"github.com/pagarme/teleport/database"
)

var db *database.Database
var batcher *Batcher

func init() {
	gob.Register(&StubAction{})

	config := config.New()
	err := config.ReadFromFile("../config_test.yml")

	if err != nil {
		fmt.Printf("Error opening config file: %v\n", err)
		os.Exit(1)
	}

	db = database.New(config.Database)

	// Start db
	if err = db.Start(); err != nil {
		fmt.Printf("Erro starting database: ", err)
		os.Exit(1)
	}

	targets := make(map[string]*client.Client)

	for key, target := range config.Targets {
		targets[key] = client.New(target)
	}

	batcher = New(db, targets, -1)
}

// StubAction implements Action
type StubAction struct {
	ShouldFilter  bool
	SeparateBatch bool
}

func (a *StubAction) Execute(c *action.Context) error {
	return nil
}

func (a *StubAction) Filter(targetExpression string) bool {
	return a.ShouldFilter
}

func (a *StubAction) NeedsSeparatedBatch() bool {
	return a.SeparateBatch
}

func TestMarkEventsBatched(t *testing.T) {
	db.Db.Exec(`
		TRUNCATE teleport.event;
		TRUNCATE teleport.batch;
	`)

	tx := db.NewTransaction()
	stubEvent := &database.Event{
		Kind:          "ddl",
		Status:        "waiting_batch",
		TriggerTag:    "TAG",
		TriggerEvent:  "EVENT",
		TransactionId: "123",
	}
	stubEvent.InsertQuery(tx)
	tx.Commit()

	tx = db.NewTransaction()

	err := batcher.markEventsBatched([]*database.Event{stubEvent}, tx)

	if err != nil {
		t.Errorf("mark events batched returned error: %#v\n", err)
	}

	tx.Commit()

	batchedEvents, _ := db.GetEvents(nil, "batched", -1)
	var updatedEvent *database.Event

	for _, event := range batchedEvents {
		if stubEvent.Id == event.Id {
			updatedEvent = event
			break
		}
	}

	if updatedEvent == nil {
		t.Errorf("ignored event => nil, want %d", stubEvent)
	}
}

func TestCreateBatchesWithActions(t *testing.T) {
	testAction := &StubAction{true, false}
	separateAction := &StubAction{true, true}

	actionsForTarget := map[string][]action.Action{
		"test_target": []action.Action{
			testAction,
			testAction,
			separateAction,
			testAction,
		},
	}

	tx := batcher.db.NewTransaction()

	batches, err := batcher.CreateBatchesWithActions(actionsForTarget, tx)

	if err != nil {
		t.Errorf("create batches returned error: %#v", err)
	}

	if len(batches) != 3 {
		t.Errorf("batches => %d, want %d", len(batches), 3)
	}

	expectedActions := [][]action.Action{
		[]action.Action{
			testAction,
			testAction,
		},
		[]action.Action{
			separateAction,
		},
		[]action.Action{
			testAction,
		},
	}

	for i, batch := range batches {
		actions, _ := batch.GetActions()

		if !reflect.DeepEqual(expectedActions[i], actions) {
			t.Errorf(
				"actions for batch %i => %#v, want %#v",
				i,
				actions,
				expectedActions[i],
			)
		}
	}
}

func TestActionsForTarget(t *testing.T) {
	batcher.db.Schemas = map[string]*database.Schema{
		"public": &database.Schema{
			Tables: []*database.Table{
				&database.Table{
					RelationKind: "r",
					RelationName: "test_table",
					Columns: []*database.Column{
						&database.Column{
							Name:         "id",
							Num:          1,
							TypeName:     "int4",
							TypeSchema:   "pg_catalog",
							TypeOid:      "123",
							IsPrimaryKey: true,
							Table:        nil,
						},
						&database.Column{
							Name:         "content",
							Num:          2,
							TypeName:     "text",
							TypeSchema:   "pg_catalog",
							TypeOid:      "124",
							IsPrimaryKey: false,
							Table:        nil,
						},
					},
				},
			},
		},
	}

	dataEvent1 := `{
		"pre":[{"oid":"2200","schema_name":"public","owner_id":"10","classes":
			[{"oid":"16443","namespace_oid":"2200","relation_kind":"r","relation_name":"test_table","columns":
				[
					{"class_oid":"16443","attr_name":"id","attr_num":1,"type_name":"int4","type_oid":"23","is_primary_key":true,"type_schema":"pg_catalog"}
				]
			}]
		}],
		"post":[{"oid":"2200","schema_name":"public","owner_id":"10","classes":
			[{"oid":"16443","namespace_oid":"2200","relation_kind":"r","relation_name":"test_table","columns":
				[
					{"class_oid":"16443","attr_name":"id","attr_num":1,"type_name":"int4","type_oid":"23","is_primary_key":true,"type_schema":"pg_catalog"},
					{"class_oid":"16443","attr_name":"content","attr_num":2,"type_name":"text","type_oid":"24","type_schema":"pg_catalog"}
				]
			}]
		}]
	}`

	dataEvent2 := `{
		"pre":null,
		"post":{
			"id": 5
		}
	}`

	events := database.Events{
		&database.Event{
			Kind:          "ddl",
			Status:        "waiting_batch",
			TriggerTag:    "TAG",
			TriggerEvent:  "EVENT",
			TransactionId: "123",
			Data:          &dataEvent1,
		},
		&database.Event{
			Kind:          "dml",
			Status:        "waiting_batch",
			TriggerTag:    "public.test_table",
			TriggerEvent:  "INSERT",
			TransactionId: "123",
			Data:          &dataEvent2,
		},
	}

	actionsForTargets, err := batcher.actionsForTargets(events)

	if err != nil {
		t.Errorf("actions for targets returned error: %#v", err)
	}

	expectedActions := map[string][]action.Action{
		"test_target": []action.Action{
			&action.CreateColumn{
				SchemaName: "live",
				TableName:  "test_table",
				Column: action.Column{
					Name:         "content",
					Type:         "text",
					IsNativeType: true,
				},
			},
			&action.InsertRow{
				SchemaName:     "live",
				TableName:      "test_table",
				PrimaryKeyName: "id",
				Rows: action.Rows{
					action.Row{
						Value: 5,
						Column: action.Column{
							Name:         "id",
							Type:         "int4",
							IsNativeType: true,
						},
					},
				},
			},
		},
	}

	if diff := pretty.Compare(expectedActions, actionsForTargets); diff != "" {
		t.Errorf(
			"actions for target => %s",
			diff,
		)
	}
}
