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
		nil,
	}

	defaultContext = ddldiff.Context{
		Schema: "default_context",
	}
}

func TestFunctionDiff(t *testing.T) {
	var tests = []struct {
		pre    *Function
		post   *Function
		output []action.Action
	}{
		{
			// Diff a index creation
			nil,
			&Function{
				"123",
				"test_fn",
				"create function test_fn()",
				"",
				schema,
			},
			[]action.Action{
				&action.CreateFunction{
					"default_context",
					"test_fn",
					"create function test_fn()",
				},
			},
		},
		{
			// Diff a index rename
			&Function{
				"123",
				"test_fn",
				"create function test_fn()",
				"args1",
				schema,
			},
			&Function{
				"123",
				"test_fn_2",
				"create function test_fn_2()",
				"args2",
				schema,
			},
			[]action.Action{
				&action.AlterFunction{
					"default_context",
					"args1",
					"test_fn",
					"test_fn_2",
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

func TestFunctionChildren(t *testing.T) {
	function := &Function{
		"123",
		"test_fn_2",
		"create function test_fn_2()",
		"args2",
		schema,
	}

	children := function.Children()

	if len(children) != 0 {
		t.Errorf("function children => %d, want %d", len(children), 0)
	}
}

func TestFunctionDrop(t *testing.T) {
	function := &Function{
		"123",
		"test_fn_2",
		"create function test_fn_2()",
		"args2",
		schema,
	}

	actions := function.Drop(defaultContext)

	if len(actions) != 1 {
		t.Errorf("actions => %d, want %d", len(actions), 1)
	}

	dropAction, ok := actions[0].(*action.DropFunction)

	if !ok {
		t.Errorf("action is not DropFunction")
	}

	if dropAction.SchemaName != defaultContext.Schema {
		t.Errorf("drop action schema name => %s, want %s", dropAction.SchemaName, defaultContext.Schema)
	}

	if dropAction.FunctionName != function.Name {
		t.Errorf("drop action function name => %s, want %s", dropAction.FunctionName, function.Name)
	}

	if dropAction.Arguments != function.Arguments {
		t.Errorf("drop action arguments => %s, want %s", dropAction.Arguments, function.Arguments)
	}
}

func TestFunctionIsEqual(t *testing.T) {
	pre := &Function{
		"123",
		"test_fn",
		"create function test_fn_2()",
		"args2",
		schema,
	}

	post := &Function{
		"123",
		"test_fn_2",
		"create function test_fn_2()",
		"args2",
		schema,
	}

	if !post.IsEqual(pre) {
		t.Errorf("expect classes to be equal")
	}

	post.Name = pre.Name
	post.Oid = "456"

	if post.IsEqual(pre) {
		t.Errorf("expect classes not to be equal")
	}

	preOtherType := &Attribute{
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
