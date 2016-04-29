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

	typ = &Type{
		"789",
		"test_type",
		"c",
		[]*Enum{},
		[]*Attribute{},
		schema,
	}

	defaultContext = ddldiff.Context{
		Schema: "default_context",
	}
}

func TestAttributeDiff(t *testing.T) {
	var tests = []struct {
		pre    *Attribute
		post   *Attribute
		output []action.Action
	}{
		{
			// Diff a column creation
			nil,
			&Attribute{
				"test_col",
				1,
				"text",
				"pg_catalog",
				"0",
				typ,
			},
			[]action.Action{
				&action.CreateAttribute{
					"default_context",
					"test_type",
					action.Column{
						"test_col",
						"text",
						true,
					},
				},
			},
		},
		{
			// Diff a column update
			&Attribute{
				"test_col",
				1,
				"text",
				"pg_catalog",
				"0",
				typ,
			},
			&Attribute{
				"test_col_2",
				1,
				"json",
				"pg_catalog",
				"0",
				typ,
			},
			[]action.Action{
				&action.AlterAttribute{
					"default_context",
					"test_type",
					action.Column{
						"test_col",
						"text",
						true,
					},
					action.Column{
						"test_col_2",
						"json",
						true,
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

func TestAttributeChildren(t *testing.T) {
	attr := &Attribute{
		"test_col_2",
		1,
		"json",
		"pg_catalog",
		"0",
		typ,
	}

	children := attr.Children()

	if len(children) != 0 {
		t.Errorf("attr children => %d, want %d", len(children), 0)
	}
}

func TestAttributeDrop(t *testing.T) {
	attr := &Attribute{
		"test_col_2",
		1,
		"json",
		"pg_catalog",
		"0",
		typ,
	}

	actions := attr.Drop(defaultContext)

	if len(actions) != 1 {
		t.Errorf("actions => %d, want %d", len(actions), 1)
	}

	dropAction, ok := actions[0].(*action.DropAttribute)

	if !ok {
		t.Errorf("action is not DropAttribute")
	}

	if dropAction.SchemaName != defaultContext.Schema {
		t.Errorf("drop action schema name => %s, want %s", dropAction.SchemaName, defaultContext.Schema)
	}

	if dropAction.TypeName != typ.Name {
		t.Errorf("drop action table name => %s, want %s", dropAction.TypeName, typ.Name)
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
		"pg_catalog",
		"0",
		typ,
	}

	post := &Attribute{
		"test_col_2",
		1,
		"int4",
		"pg_catalog",
		"0",
		typ,
	}

	if !post.IsEqual(pre) {
		t.Errorf("expect classes to be equal")
	}

	post.Name = pre.Name
	post.Num = 2

	if post.IsEqual(pre) {
		t.Errorf("expect classes not to be equal")
	}

	preOtherType := &Enum{
		"123",
		"test_enum1_renamed",
		typ,
	}

	if post.IsEqual(preOtherType) {
		t.Errorf("expect two different types not to be equal")
	}
}
