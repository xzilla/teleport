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

	defaultContext = ddldiff.Context{
		Schema: "default_context",
	}
}

func TestTypeDiff(t *testing.T) {
	var tests = []struct {
		pre    *Type
		post   *Type
		output []action.Action
	}{
		{
			// Diff a type creation
			nil,
			&Type{
				"789",
				"test_type",
				"e",
				[]*Enum{
					&Enum{
						"123",
						"test_enum1",
						nil,
					},
					&Enum{
						"124",
						"test_enum2",
						nil,
					},
				},
				[]*Attribute{},
				schema,
			},
			[]action.Action{
				&action.CreateType{
					"default_context",
					"test_type",
					"e",
				},
			},
		},
		{
			// Diff a type rename
			&Type{
				"789",
				"test_type",
				"e",
				[]*Enum{
					&Enum{
						"123",
						"test_enum1",
						nil,
					},
					&Enum{
						"124",
						"test_enum2",
						nil,
					},
				},
				[]*Attribute{},
				schema,
			},
			&Type{
				"789",
				"test_type_renamed",
				"e",
				[]*Enum{
					&Enum{
						"123",
						"test_enum1",
						nil,
					},
					&Enum{
						"124",
						"test_enum2",
						nil,
					},
				},
				[]*Attribute{},
				schema,
			},
			[]action.Action{
				&action.AlterType{
					"default_context",
					"test_type",
					"test_type_renamed",
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

func TestTypeChildren(t *testing.T) {
	enums := []*Enum{
		&Enum{
			"123",
			"test_enum1",
			nil,
		},
	}

	attrs := []*Attribute{
		&Attribute{
			"test_col",
			1,
			"text",
			"pg_catalog",
			"0",
			nil,
		},
	}

	typ := &Type{
		"789",
		"test_type",
		"c",
		enums,
		attrs,
		nil,
	}

	children := typ.Children()

	if len(children) != 2 {
		t.Errorf("children => %d, want %d", len(children), 1)
	}

	if children[0] != enums[0] {
		t.Errorf("child 0 => %v, want %v", children[0], enums[0])
	}

	if children[1] != attrs[0] {
		t.Errorf("child 1 => %v, want %v", children[1], attrs[0])
	}
}

func TestTypeDrop(t *testing.T) {
	typ := &Type{
		"789",
		"test_type",
		"c",
		[]*Enum{
			&Enum{
				"123",
				"test_enum1",
				nil,
			},
		},
		[]*Attribute{},
		schema,
	}

	actions := typ.Drop(defaultContext)

	if len(actions) != 1 {
		t.Errorf("actions => %d, want %d", len(actions), 1)
	}

	dropAction, ok := actions[0].(*action.DropType)

	if !ok {
		t.Errorf("action is not DropType")
	}

	if dropAction.SchemaName != defaultContext.Schema {
		t.Errorf("drop action schema name => %s, want %s", dropAction.SchemaName, defaultContext.Schema)
	}

	if dropAction.TypeName != typ.Name {
		t.Errorf("drop action type name => %s, want %s", dropAction.TypeName, typ.Name)
	}
}

func TestTypeIsEqual(t *testing.T) {
	pre := &Type{
		"789",
		"test_type",
		"c",
		[]*Enum{
			&Enum{
				"123",
				"test_enum1",
				nil,
			},
			&Enum{
				"124",
				"test_enum2",
				nil,
			},
		},
		[]*Attribute{},
		schema,
	}

	post := &Type{
		"789",
		"test_type_renamed",
		"c",
		[]*Enum{
			&Enum{
				"123",
				"test_enum1",
				nil,
			},
			&Enum{
				"124",
				"test_enum2",
				nil,
			},
		},
		[]*Attribute{},
		schema,
	}

	preOtherType := &Table{
		"123",
		"r",
		"test_table",
		[]*Column{
			&Column{
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

	if !post.IsEqual(pre) {
		t.Errorf("expect types to be equal")
	}

	post.Name = pre.Name
	post.Oid = "890"

	if post.IsEqual(pre) {
		t.Errorf("expect types not to be equal")
	}

	if post.IsEqual(preOtherType) {
		t.Errorf("expect two different types not to be equal")
	}
}
