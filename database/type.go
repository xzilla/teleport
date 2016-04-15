package database

import (
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/batcher/ddldiff"
)

type Type struct {
	Oid string `json:"oid"`
	Name string `json:"type_name"`
	Enums []*Enum `json:"enums"`
	Schema       *Schema
}

func (post *Type) Diff(other ddldiff.Diffable) []action.Action {
	actions := make([]action.Action, 0)

	if other == nil {
		enumsStr := make([]string, 0)

		for _, enum := range post.Enums {
			enumsStr = append(enumsStr, enum.Name)
		}

		actions = append(actions, &action.CreateType{
			post.Schema.Name,
			post.Name,
			enumsStr,
		})
	} else {
		pre := other.(*Type)

		if pre.Name != post.Name {
			actions = append(actions, &action.AlterType{
				post.Schema.Name,
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

	return children
}

func (t *Type) Drop() []action.Action {
	return []action.Action{
		&action.DropType{
			t.Schema.Name,
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
