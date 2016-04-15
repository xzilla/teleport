package database

import (
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/batcher/ddldiff"
)

type Enum struct {
	Oid  string `json:"oid"`
	Name string `json:"name"`
	Type *Type
}

func (post *Enum) Diff(other ddldiff.Diffable) []action.Action {
	actions := make([]action.Action, 0)

	if other == nil {
		actions = append(actions, &action.CreateEnum{
			post.Type.Schema.Name,
			post.Type.Name,
			post.Name,
		})
	}

	return actions
}

func (e *Enum) Children() []ddldiff.Diffable {
	return []ddldiff.Diffable{}
}

func (e *Enum) Drop() []action.Action {
	return []action.Action{}
}

func (e *Enum) IsEqual(other ddldiff.Diffable) bool {
	if other == nil {
		return false
	}

	otherEnum := other.(*Enum)
	return (e.Oid == otherEnum.Oid)
}
