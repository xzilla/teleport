package database

import (
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/batcher/ddldiff"
)

type Extension struct {
	Oid    string `json:"oid"`
	Name   string `json:"extension_name"`
	Schema *Schema
}

func (post *Extension) Diff(other ddldiff.Diffable, context ddldiff.Context) []action.Action {
	actions := make([]action.Action, 0)

	if other == nil {
		actions = append(actions, &action.CreateExtension{
			context.Schema,
			post.Name,
		})
	}

	return actions
}

func (e *Extension) Children() []ddldiff.Diffable {
	return []ddldiff.Diffable{}
}

func (e *Extension) Drop(context ddldiff.Context) []action.Action {
	return []action.Action{
		&action.DropExtension{
			e.Name,
		},
	}
}

func (e *Extension) IsEqual(other ddldiff.Diffable) bool {
	if other == nil {
		return false
	}

	if otherExtension, ok := other.(*Extension); ok {
		return (e.Oid == otherExtension.Oid)
	}

	return false
}
