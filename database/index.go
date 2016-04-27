package database

import (
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/batcher/ddldiff"
)

// Define a class column
type Index struct {
	Oid   string `json:"index_oid"`
	Name  string `json:"index_name"`
	Def   string `json:"index_def"`
	Table *Table
}

// Implements Diffable
func (post *Index) Diff(other ddldiff.Diffable, context ddldiff.Context) []action.Action {
	actions := make([]action.Action, 0)

	if other == nil {
		actions = append(actions, &action.CreateIndex{
			context.Schema,
			post.Name,
			post.Def,
		})
	} else {
		pre := other.(*Index)

		if pre.Name != post.Name {
			actions = append(actions, &action.AlterIndex{
				context.Schema,
				pre.Name,
				post.Name,
			})
		}
	}

	return actions
}

func (i *Index) Children() []ddldiff.Diffable {
	return []ddldiff.Diffable{}
}

func (i *Index) Drop(context ddldiff.Context) []action.Action {
	return []action.Action{
		&action.DropIndex{
			context.Schema,
			i.Name,
		},
	}
}

func (i *Index) IsEqual(other ddldiff.Diffable) bool {
	if other == nil {
		return false
	}

	if otherIndex, ok := other.(*Index); ok {
		return (i.Oid == otherIndex.Oid)
	}

	return false
}
