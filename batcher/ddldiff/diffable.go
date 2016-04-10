package ddldiff

import (
	"github.com/pagarme/teleport/batcher/ddlaction"
)

type Diffable interface {
	// Diff with other Diffable
	Diff(other Diffable) []ddlaction.Action
	// Get other diffables inside the current one
	Children() []Diffable
	// Return actions to drop the current diffable
	Drop() []ddlaction.Action
	// Compare current Diffable with other
	IsEqual(other Diffable) bool
}
