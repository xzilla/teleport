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
		[]*Class{},
		nil,
		nil,
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
				schema,
			},
			[]action.Action{
				&action.CreateType{
					"test_schema",
					"test_type",
				},
			},
		},
		{
			// Diff a type rename
			&Type{
				"789",
				"test_type",
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
				schema,
			},
			&Type{
				"789",
				"test_type_renamed",
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
				schema,
			},
			[]action.Action{
				&action.AlterType{
					"test_schema",
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

func TestTypeChildren(t *testing.T) {
	enums := []*Enum{
		&Enum{
			"123",
			"test_enum1",
			nil,
		},
	}

	typ := &Type{
		"789",
		"test_type",
		enums,
		nil,
	}

	children := typ.Children()

	if len(children) != 1 {
		t.Errorf("children => %d, want %d", len(children), 1)
	}

	for i, child := range children {
		if child != enums[i] {
			t.Errorf("child %i => %v, want %v", i, child, enums[i])
		}
	}
}

func TestTypeDrop(t *testing.T) {
	typ := &Type{
		"789",
		"test_type",
		[]*Enum{
			&Enum{
				"123",
				"test_enum1",
				nil,
			},
		},
		schema,
	}

	actions := typ.Drop()

	if len(actions) != 1 {
		t.Errorf("actions => %d, want %d", len(actions), 1)
	}

	dropAction, ok := actions[0].(*action.DropType)

	if !ok {
		t.Errorf("action is not DropType")
	}

	if dropAction.SchemaName != schema.Name {
		t.Errorf("drop action schema name => %s, want %s", dropAction.SchemaName, schema.Name)
	}

	if dropAction.TypeName != typ.Name {
		t.Errorf("drop action type name => %s, want %s", dropAction.TypeName, typ.Name)
	}
}

func TestTypeIsEqual(t *testing.T) {
	pre := &Type{
		"789",
		"test_type",
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
		schema,
	}

	post := &Type{
		"789",
		"test_type_renamed",
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
		schema,
	}

	preOtherType := &Class{
		"123",
		"r",
		"test_table",
		[]*Attribute{
			&Attribute{
				"test_col",
				1,
				"int4",
				"0",
				false,
				nil,
			},
		},
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
