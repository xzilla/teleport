package database

import (
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/batcher/ddldiff"
	"reflect"
	"testing"
)

var class *Class

func init() {
	schema = &Schema{
		"123",
		"test_schema",
		[]*Class{},
		nil,
		nil,
	}

	class = &Class{
		"123",
		"r",
		"test_table",
		[]*Attribute{},
		schema,
	}
}

func TestAttributeDiff(t *testing.T) {
	var tests = []struct {
		pre    *Attribute
		post   *Attribute
		output []action.Action
	}{
		{
			// Diff a attribute creation
			nil,
			&Attribute{
				"test_col",
				1,
				"text",
				"0",
				false,
				class,
			},
			[]action.Action{
				&action.CreateColumn{
					"test_schema",
					"test_table",
					action.Column{
						"test_col",
						"text",
					},
				},
			},
		},
		{
			// Diff a attribute update
			&Attribute{
				"test_col",
				1,
				"text",
				"0",
				false,
				class,
			},
			&Attribute{
				"test_col_2",
				1,
				"int4",
				"0",
				false,
				class,
			},
			[]action.Action{
				&action.AlterColumn{
					"test_schema",
					"test_table",
					action.Column{
						"test_col",
						"text",
					},
					action.Column{
						"test_col_2",
						"int4",
					},
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

		actions := test.post.Diff(preObj)

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

func TestAttributeChildren(t *testing.T) {
	attr := &Attribute{
		"test_col",
		1,
		"text",
		"0",
		false,
		class,
	}

	children := attr.Children()

	if len(children) != 0 {
		t.Errorf("attr children => %d, want %d", len(children), 0)
	}
}

func TestAttributeDrop(t *testing.T) {
	attr := &Attribute{
		"test_col",
		1,
		"text",
		"0",
		false,
		class,
	}

	actions := attr.Drop()

	if len(actions) != 1 {
		t.Errorf("actions => %d, want %d", len(actions), 1)
	}

	dropAction, ok := actions[0].(*action.DropColumn)

	if !ok {
		t.Errorf("action is not DropColumn")
	}

	if dropAction.SchemaName != schema.Name {
		t.Errorf("drop action schema name => %s, want %s", dropAction.SchemaName, schema.Name)
	}

	if dropAction.TableName != class.RelationName {
		t.Errorf("drop action table name => %s, want %s", dropAction.TableName, class.RelationName)
	}

	if dropAction.Column.Name != attr.Name {
		t.Errorf("drop action column name => %s, want %s", dropAction.Column.Name, attr.Name)
	}

	if dropAction.Column.Type != attr.TypeName {
		t.Errorf("drop action column name => %s, want %s", dropAction.Column.Type, attr.TypeName)
	}
}

func TestAttributeIsEqual(t *testing.T) {
	pre := &Attribute{
		"test_col",
		1,
		"text",
		"0",
		false,
		class,
	}

	post := &Attribute{
		"test_col_2",
		1,
		"int4",
		"0",
		false,
		class,
	}

	if !post.IsEqual(pre) {
		t.Errorf("expect classes to be equal")
	}

	post.Name = pre.Name
	post.Num = 2

	if post.IsEqual(pre) {
		t.Errorf("expect classes not to be equal")
	}
}
