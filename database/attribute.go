package database

import (
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/batcher/ddldiff"
)

// Define a class column
type Attribute struct {
	Name       string `json:"attr_name"`
	Num        int    `json:"attr_num"`
	TypeName   string `json:"type_name"`
	TypeSchema string `json:"type_schema"`
	TypeOid    string `json:"type_oid"`
	Type       *Type
}

func (a *Attribute) IsNativeType() bool {
	return a.TypeSchema == "pg_catalog"
}

// Implements Diffable
func (post *Attribute) Diff(other ddldiff.Diffable, context ddldiff.Context) []action.Action {
	actions := make([]action.Action, 0)

	if other == nil {
		actions = append(actions, &action.CreateAttribute{
			context.Schema,
			post.Type.Name,
			action.Column{
				post.Name,
				post.TypeName,
				post.IsNativeType(),
			},
		})
	} else {
		pre := other.(*Attribute)

		if pre.Name != post.Name || pre.TypeOid != post.TypeOid {
			actions = append(actions, &action.AlterAttribute{
				context.Schema,
				post.Type.Name,
				action.Column{
					pre.Name,
					pre.TypeName,
					pre.IsNativeType(),
				},
				action.Column{
					post.Name,
					post.TypeName,
					post.IsNativeType(),
				},
			})
		}
	}

	return actions
}

func (a *Attribute) Children() []ddldiff.Diffable {
	return []ddldiff.Diffable{}
}

func (a *Attribute) Drop(context ddldiff.Context) []action.Action {
	return []action.Action{
		&action.DropAttribute{
			context.Schema,
			a.Type.Name,
			action.Column{
				a.Name,
				a.TypeName,
				a.IsNativeType(),
			},
		},
	}
}

func (a *Attribute) IsEqual(other ddldiff.Diffable) bool {
	if other == nil {
		return false
	}

	if otherAttr, ok := other.(*Attribute); ok {
		return (a.Num == otherAttr.Num)
	}

	return false
}
