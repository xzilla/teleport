package database

import (
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/batcher/ddldiff"
)

type Type struct {
	Oid    string  `json:"oid"`
	Name   string  `json:"type_name"`
	Type   string  `json:"type_type"`
	Enums  []*Enum `json:"enums"`
	Attributes  []*Attribute `json:"attributes"`
	Schema *Schema
}

func (post *Type) Diff(other ddldiff.Diffable, context ddldiff.Context) []action.Action {
	actions := make([]action.Action, 0)

	if other == nil {
		actions = append(actions, &action.CreateType{
			context.Schema,
			post.Name,
			post.Type,
		})
	} else {
		pre := other.(*Type)

		if pre.Name != post.Name {
			actions = append(actions, &action.AlterType{
				context.Schema,
				pre.Name,
				post.Name,
			})
		}
	}

	return actions
}

func (t *Type) Children() []ddldiff.Diffable {
	children := make([]ddldiff.Diffable, 0)

	for _, enum := range t.Enums {
		children = append(children, enum)
	}

	for _, attr := range t.Attributes {
		children = append(children, attr)
	}

	return children
}

func (t *Type) Drop(context ddldiff.Context) []action.Action {
	return []action.Action{
		&action.DropType{
			context.Schema,
			t.Name,
		},
	}
}

func (t *Type) IsEqual(other ddldiff.Diffable) bool {
	if other == nil {
		return false
	}

	if otherType, ok := other.(*Type); ok {
		return (t.Oid == otherType.Oid)
	}

	return false
}
