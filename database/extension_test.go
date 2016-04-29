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

func TestExtensionDiff(t *testing.T) {
	var tests = []struct {
		pre    *Extension
		post   *Extension
		output []action.Action
	}{
		{
			// Diff a extension creation
			nil,
			&Extension{
				"123",
				"test_ext_5",
				schema,
			},
			[]action.Action{
				&action.CreateExtension{
					"default_context",
					"test_ext_5",
				},
			},
		},
		{
			// Diff a extension rename
			&Extension{
				"123",
				"test_ext",
				schema,
			},
			&Extension{
				"123",
				"test_ext_renamed",
				schema,
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
				"diff %#v with %#v => %#v, want %#v",
				test.pre,
				test.post,
				actions,
				test.output,
			)
		}
	}
}

func TestExtensionChildren(t *testing.T) {
	extension := &Extension{
		"123",
		"test_ext",
		schema,
	}

	children := extension.Children()

	if len(children) != 0 {
		t.Errorf("children => %d, want %d", len(children), 0)
	}
}

func TestExtensionDrop(t *testing.T) {
	extension := &Extension{
		"123",
		"test_ext",
		schema,
	}

	actions := extension.Drop(defaultContext)

	if len(actions) != 1 {
		t.Errorf("actions => %d, want %d", len(actions), 1)
	}

	dropAction, ok := actions[0].(*action.DropExtension)

	if !ok {
		t.Errorf("action is not DropExtension")
	}

	if dropAction.ExtensionName != extension.Name {
		t.Errorf("drop action extension name => %s, want %s", dropAction.ExtensionName, extension.Name)
	}
}

func TestExtensionIsEqual(t *testing.T) {
	pre := &Extension{
		"123",
		"test_ext",
		schema,
	}

	post := &Extension{
		"123",
		"test_ext_2",
		schema,
	}

	if !post.IsEqual(pre) {
		t.Errorf("expect enums to be equal")
	}

	post.Name = pre.Name
	post.Oid = "124"

	if post.IsEqual(pre) {
		t.Errorf("expect enums not to be equal")
	}

	preOtherType := &Type{
		"123",
		"test_type_renamed",
		"c",
		[]*Enum{},
		[]*Attribute{},
		schema,
	}

	if post.IsEqual(preOtherType) {
		t.Errorf("expect two different types not to be equal")
	}
}
