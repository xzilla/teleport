package database

import (
	"testing"
	"reflect"
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/batcher/ddldiff"
)

var schema *Schema

func init() {
	schema = &Schema{
		"123",
		"test_schema",
		[]*Class{},
	}
}

func TestClassDiff(t *testing.T) {
	var tests = []struct {
		pre *Class
		post *Class
		output []action.Action
	}{
		{
			// Diff a special table (not ordinary)
			// No output is expected since diff only diffs tables
			&Class{
				"123",
				"s",
				"test_special_table",
				[]*Attribute{},
				schema,
			},
			&Class{
				"123",
				"s",
				"test_special_table_renamed",
				[]*Attribute{},
				schema,
			},
			[]action.Action{},
		},
		{
			// Diff a table creation
			nil,
			&Class{
				"123",
				"r",
				"test_table",
				[]*Attribute{
					&Attribute{
						"test_col",
						1,
						"int4",
						"0",
						nil,
					},
				},
				schema,
			},
			[]action.Action{
				&action.CreateTable{
					"test_schema",
					"test_table",
					[]action.Column{
						action.Column{
							"test_col",
							"int4",
						},
					},
				},
			},
		},
		{
			// Diff a table rename
			&Class{
				"123",
				"r",
				"test_table",
				[]*Attribute{
					&Attribute{
						"test_col",
						1,
						"int4",
						"0",
						nil,
					},
				},
				schema,
			},
			&Class{
				"123",
				"r",
				"test_table_renamed",
				[]*Attribute{
					&Attribute{
						"test_col",
						1,
						"int4",
						"0",
						nil,
					},
				},
				schema,
			},
			[]action.Action{
				&action.AlterTable{
					"test_schema",
					"test_table",
					"test_table_renamed",
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

func TestClassChildren(t *testing.T) {
	attrs := []*Attribute {
		&Attribute{
			"test_col",
			1,
			"int4",
			"0",
			nil,
		},
	}

	class := &Class{
		"123",
		"r",
		"test_table",
		attrs,
		schema,
	}

	children := class.Children()

	if len(children) != 1 {
		t.Errorf("children => %d, want %d", len(children), 1)
	}

	for i, child := range children {
		if child != attrs[i] {
			t.Errorf("child %i => %v, want %v", i, child, attrs[i])
		}
	}
}

func TestClassDrop(t *testing.T) {
	class := &Class{
		"123",
		"s",
		"test_special_table",
		[]*Attribute{},
		schema,
	}

	actions := class.Drop()

	if len(actions) != 0 {
		t.Errorf("actions => %d, want %d", len(actions), 0)
	}

	class = &Class{
		"123",
		"r",
		"test_table_renamed",
		[]*Attribute{
			&Attribute{
				"test_col",
				1,
				"int4",
				"0",
				nil,
			},
		},
		schema,
	}

	actions = class.Drop()

	if len(actions) != 1 {
		t.Errorf("actions => %d, want %d", len(actions), 1)
	}

	dropAction, ok := actions[0].(*action.DropTable)

	if !ok {
		t.Errorf("action is not DropTable")
	}

	if dropAction.SchemaName != schema.Name {
		t.Errorf("drop action schema name => %s, want %s", dropAction.SchemaName, schema.Name)
	}

	if dropAction.TableName != class.RelationName {
		t.Errorf("drop action table name => %s, want %s", dropAction.TableName, class.RelationName)
	}
}

func TestClassIsEqual(t *testing.T) {
	pre := &Class{
		"123",
		"r",
		"test_table",
		[]*Attribute{
			&Attribute{
				"test_col",
				1,
				"int4",
				"0",
				nil,
			},
		},
		schema,
	}

	post := &Class{
		"123",
		"r",
		"test_table_renamed",
		[]*Attribute{
			&Attribute{
				"test_col",
				1,
				"int4",
				"0",
				nil,
			},
		},
		schema,
	}

	if !post.IsEqual(pre) {
		t.Errorf("expect classes to be equal")
	}

	post.RelationName = pre.RelationName
	post.Oid = "1235"

	if post.IsEqual(pre) {
		t.Errorf("expect classes not to be equal")
	}
}
