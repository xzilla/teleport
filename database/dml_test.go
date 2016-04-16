package database

import (
	"github.com/pagarme/teleport/action"
	"reflect"
	"testing"
)

var event *Event

func setupDml() {
	db = New("", "", "", "", "", 0)

	attributes := []*Attribute{
		&Attribute{
			"id",
			1,
			"int4",
			"0",
			true,
			nil,
		},
		&Attribute{
			"content",
			1,
			"text",
			"0",
			false,
			nil,
		},
	}
	
	classes := []*Class{
		&Class{
			"123",
			"r",
			"test_table",
			attributes,
			nil,
		},
	}

	db.Schemas = map[string]*Schema{
		"public": &Schema{
			"123",
			"public",
			classes,
			nil,
			db,
		},
	}

	event = &Event{
		Id:            "1",
		Kind:          "dml",
		Status:        "waiting_batch",
		TriggerTag:    "public.test_table",
		TriggerEvent:  "UPDATE",
		TransactionId: "0",
		Data:          nil,
	}
}

func TestDmlDiff(t *testing.T) {
	setupDml()

	var tests = []struct {
		dml *Dml
		output []action.Action
	}{
		{
			// Test insert a row
			&Dml{
				nil,
				&map[string]interface{}{
					"id": 5,
					"content": "test",
				},
				event,
				db,
			},
			[]action.Action{
				&action.InsertRow{
					"public",
					"test_table",
					[]action.Row{
						action.Row{
							5,
							action.Column{
								"id",
								"int4",
							},
						},
						action.Row{
							"test",
							action.Column{
								"content",
								"text",
							},
						},
					},
				},
			},
		},
		{
			// Test delete a row
			&Dml{
				&map[string]interface{}{
					"id": 5,
					"content": "test",
				},
				nil,
				event,
				db,
			},
			[]action.Action{
				&action.DeleteRow{
					"public",
					"test_table",
					action.Row{
						5,
						action.Column{
							"id",
							"int4",
						},
					},
				},
			},
		},
		{
			// Test update a row
			&Dml{
				&map[string]interface{}{
					"id": 5,
					"content": "test",
				},
				&map[string]interface{}{
					"id": 5,
					"content": "test updated",
				},
				event,
				db,
			},
			[]action.Action{
				&action.UpdateRow{
					"public",
					"test_table",
					action.Row{
						5,
						action.Column{
							"id",
							"int4",
						},
					},
					[]action.Row{
						action.Row{
							5,
							action.Column{
								"id",
								"int4",
							},
						},
						action.Row{
							"test updated",
							action.Column{
								"content",
								"text",
							},
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		actions := test.dml.Diff()

		if !reflect.DeepEqual(actions, test.output) {
			t.Errorf(
				"diff %#v => %#v, want %#v",
				test.dml,
				actions,
				test.output,
			)
		}
	}
}
