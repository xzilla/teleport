package database

import (
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/batcher/ddldiff"
	"reflect"
	"testing"
)

func init() {
	schema = &Schema{
		"123",
		"test_schema",
		[]*Table{},
		nil,
		nil,
		nil,
		nil,
	}

	class = &Table{
		"123",
		"r",
		"test_table",
		[]*Column{},
		[]*Index{},
		schema,
	}

	defaultContext = ddldiff.Context{
		Schema: "default_context",
	}
}

func TestIndexDiff(t *testing.T) {
	var tests = []struct {
		pre    *Index
		post   *Index
		output []action.Action
	}{
		{
			// Diff a index creation
			nil,
			&Index{
				"123",
				"test_index",
				"create index",
				class,
			},
			[]action.Action{
				&action.CreateIndex{
					"default_context",
					"test_index",
					"create index",
				},
			},
		},
		{
			// Diff a index rename
			&Index{
				"123",
				"test_index",
				"create index",
				class,
			},
			&Index{
				"123",
				"test_index_2",
				"create index",
				class,
			},
			[]action.Action{
				&action.AlterIndex{
					"default_context",
					"test_index",
					"test_index_2",
				},
			},
		},
	}

	for _, test := range tests {
		// Avoid passing a interface with nil pointer
		// to Diff and breaking comparisons with nil.
		var preObj ddldiff.Diffable
		if test.pre == nil {
			preObj = nil
		} else {
			preObj = test.pre
		}

		actions := test.post.Diff(preObj, defaultContext)

		if !reflect.DeepEqual(actions, test.output) {
			t.Errorf(
				"diff %#v with %#v => %v, want %d",
				test.pre,
				test.post,
				actions,
				test.output,
			)
		}
	}
}

func TestIndexChildren(t *testing.T) {
	index := &Index{
		"123",
		"test_index_2",
		"create index",
		class,
	}

	children := index.Children()

	if len(children) != 0 {
		t.Errorf("index children => %d, want %d", len(children), 0)
	}
}

func TestIndexDrop(t *testing.T) {
	index := &Index{
		"123",
		"test_index_2",
		"create index",
		class,
	}

	actions := index.Drop(defaultContext)

	if len(actions) != 1 {
		t.Errorf("actions => %d, want %d", len(actions), 1)
	}

	dropAction, ok := actions[0].(*action.DropIndex)

	if !ok {
		t.Errorf("action is not DropIndex")
	}

	if dropAction.SchemaName != defaultContext.Schema {
		t.Errorf("drop action schema name => %s, want %s", dropAction.SchemaName, defaultContext.Schema)
	}

	if dropAction.IndexName != index.Name {
		t.Errorf("drop action table name => %s, want %s", dropAction.IndexName, index.Name)
	}
}

func TestIndexIsEqual(t *testing.T) {
	pre := &Index{
		"123",
		"test_index_2",
		"create index",
		class,
	}

	post := &Index{
		"123",
		"test_index",
		"create index",
		class,
	}

	if !post.IsEqual(pre) {
		t.Errorf("expect classes to be equal")
	}

	post.Name = pre.Name
	post.Oid = "456"

	if post.IsEqual(pre) {
		t.Errorf("expect classes not to be equal")
	}

	preOtherType := &Column{
		"test_col",
		1,
		"text",
		"pg_catalog",
		"0",
		false,
		class,
	}

	if post.IsEqual(preOtherType) {
		t.Errorf("expect two different types not to be equal")
	}
}
