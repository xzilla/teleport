package ddldiff

import (
	"github.com/pagarme/teleport/action"
)

type Diffable interface {
	// Diff with other Diffable
	Diff(other Diffable) []action.Action
	// Get other diffables inside the current one
	Children() []Diffable
	// Return actions to drop the current diffable
	Drop() []action.Action
	// Compare current Diffable with other
	IsEqual(other Diffable) bool
}
