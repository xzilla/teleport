package database

import (
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/batcher/ddldiff"
)

type Function struct {
	Oid       string `json:"oid"`
	Name      string `json:"function_name"`
	Def       string `json:"function_def"`
	Arguments string `json:"function_arguments"`
	Schema    *Schema
}

func (post *Function) Diff(other ddldiff.Diffable, context ddldiff.Context) []action.Action {
	actions := make([]action.Action, 0)

	if other == nil {
		actions = append(actions, &action.CreateFunction{
			context.Schema,
			post.Name,
			post.Def,
		})
	} else {
		pre := other.(*Function)

		if pre.Name != post.Name {
			actions = append(actions, &action.AlterFunction{
				context.Schema,
				pre.Arguments,
				pre.Name,
				post.Name,
			})
		}
	}

	return actions
}

func (f *Function) Children() []ddldiff.Diffable {
	return []ddldiff.Diffable{}
}

func (f *Function) Drop(context ddldiff.Context) []action.Action {
	return []action.Action{
		&action.DropFunction{
			context.Schema,
			f.Name,
			f.Arguments,
		},
	}
}

func (f *Function) IsEqual(other ddldiff.Diffable) bool {
	if other == nil {
		return false
	}

	if otherFunction, ok := other.(*Function); ok {
		return (f.Oid == otherFunction.Oid)
	}

	return false
}
