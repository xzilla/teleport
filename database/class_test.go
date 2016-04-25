package database

import (
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/batcher/ddldiff"
	"reflect"
	"testing"
)

var schema *Schema

func init() {
	schema = &Schema{
		"123",
		"test_schema",
		[]*Class{},
		nil,
		nil,
	}

	defaultContext = ddldiff.Context{
		Schema: "default_context",
	}
}

func TestClassDiff(t *testing.T) {
	var tests = []struct {
		pre    *Class
		post   *Class
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
				[]*Index{},
				schema,
			},
			&Class{
				"123",
				"s",
				"test_special_table_renamed",
				[]*Attribute{},
				[]*Index{},
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
						"pg_catalog",
						"0",
						true,
						nil,
					},
				},
				[]*Index{},
				schema,
			},
			[]action.Action{
				&action.CreateTable{
					"default_context",
					"test_table",
					action.Column{
						"test_col",
						"int4",
						true,
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
						"pg_catalog",
						"0",
						false,
						nil,
					},
				},
				[]*Index{},
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
						"pg_catalog",
						"0",
						false,
						nil,
					},
				},
				[]*Index{},
				schema,
			},
			[]action.Action{
				&action.AlterTable{
					"default_context",
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

func TestClassChildren(t *testing.T) {
	attrs := []*Attribute{
		&Attribute{
			"test_col",
			1,
			"int4",
			"pg_catalog",
			"0",
			true,
			nil,
		},
	}

	class := &Class{
		"123",
		"r",
		"test_table",
		attrs,
		[]*Index{},
		schema,
	}

	children := class.Children()

	// Should not return primary key
	if len(children) != 0 {
		t.Errorf("children => %d, want %d", len(children), 0)
	}

	attrs = []*Attribute{
		&Attribute{
			"test_col",
			1,
			"int4",
			"pg_catalog",
			"0",
			true,
			nil,
		},
		&Attribute{
			"other_col",
			1,
			"text",
			"pg_catalog",
			"0",
			false,
			nil,
		},
	}

	class.Attributes = attrs

	children = class.Children()

	// Should not return primary key (only the other key)
	if len(children) != 1 {
		t.Errorf("children => %d, want %d", len(children), 1)
	}

	if children[0] != attrs[1] {
		t.Errorf("child => %v, want %v", children[0], attrs[1])
	}

	attrs = []*Attribute{
		&Attribute{
			"other_col",
			1,
			"text",
			"pg_catalog",
			"0",
			false,
			nil,
		},
	}

	indexes := []*Index{
		&Index{
			"123",
			"test_index",
			"create index;",
			nil,
		},
	}

	class.Attributes = attrs
	class.Indexes = indexes

	children = class.Children()

	if len(children) != 2 {
		t.Errorf("children => %d, want %d", len(children), 2)
	}

	if children[0] != attrs[0] {
		t.Errorf("child => %v, want %v", children[0], attrs[0])
	}

	if children[1] != indexes[0] {
		t.Errorf("child => %v, want %v", children[1], indexes[0])
	}
}

func TestClassDrop(t *testing.T) {
	class := &Class{
		"123",
		"s",
		"test_special_table",
		[]*Attribute{},
		[]*Index{},
		schema,
	}

	actions := class.Drop(defaultContext)

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
				"pg_catalog",
				"0",
				false,
				nil,
			},
		},
		[]*Index{},
		schema,
	}

	actions = class.Drop(defaultContext)

	if len(actions) != 1 {
		t.Errorf("actions => %d, want %d", len(actions), 1)
	}

	dropAction, ok := actions[0].(*action.DropTable)

	if !ok {
		t.Errorf("action is not DropTable")
	}

	if dropAction.SchemaName != defaultContext.Schema {
		t.Errorf("drop action schema name => %s, want %s", dropAction.SchemaName, defaultContext.Schema)
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
				"pg_catalog",
				"0",
				false,
				nil,
			},
		},
		[]*Index{},
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
				"pg_catalog",
				"0",
				false,
				nil,
			},
		},
		[]*Index{},
		schema,
	}

	preOtherType := &Type{
		"789",
		"test_type",
		[]*Enum{},
		nil,
	}

	if !post.IsEqual(pre) {
		t.Errorf("expect classes to be equal")
	}

	post.RelationName = pre.RelationName
	post.Oid = "1235"

	if post.IsEqual(pre) {
		t.Errorf("expect classes not to be equal")
	}

	if post.IsEqual(preOtherType) {
		t.Errorf("expect two different types not to be equal")
	}
}
