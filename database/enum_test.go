package database

import (
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/batcher/ddldiff"
	"reflect"
	"testing"
)

var typ *Type

func init() {
	schema = &Schema{
		"123",
		"test_schema",
		[]*Class{},
		nil,
		nil,
		nil,
	}

	typ = &Type{
		"789",
		"test_type",
		[]*Enum{},
		schema,
	}

	defaultContext = ddldiff.Context{
		Schema: "default_context",
	}
}

func TestEnumDiff(t *testing.T) {
	var tests = []struct {
		pre    *Enum
		post   *Enum
		output []action.Action
	}{
		{
			// Diff a enum creation
			nil,
			&Enum{
				"123",
				"test_enum3",
				typ,
			},
			[]action.Action{
				&action.CreateEnum{
					"default_context",
					"test_type",
					"test_enum3",
				},
			},
		},
		{
			// Diff a enum rename
			&Enum{
				"123",
				"test_enum3",
				typ,
			},
			&Enum{
				"123",
				"test_enum3_renamed",
				typ,
			},
			[]action.Action{},
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

func TestEnumChildren(t *testing.T) {
	enum := &Enum{
		"123",
		"test_enum1",
		typ,
	}

	children := enum.Children()

	if len(children) != 0 {
		t.Errorf("children => %d, want %d", len(children), 0)
	}
}

func TestEnumDrop(t *testing.T) {
	enum := &Enum{
		"123",
		"test_enum1",
		typ,
	}

	actions := enum.Drop(defaultContext)

	if len(actions) != 0 {
		t.Errorf("actions => %d, want %d", len(actions), 0)
	}
}

func TestEnumIsEqual(t *testing.T) {
	pre := &Enum{
		"123",
		"test_enum1",
		typ,
	}

	post := &Enum{
		"123",
		"test_enum1_renamed",
		typ,
	}

	if !post.IsEqual(pre) {
		t.Errorf("expect enums to be equal")
	}

	post.Name = pre.Name
	post.Oid = "1235"

	if post.IsEqual(pre) {
		t.Errorf("expect enums not to be equal")
	}
}
