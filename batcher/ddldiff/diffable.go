package ddldiff

type Diffable interface {
	// Diff with other Diffable
	Diff(other Diffable) []Action
	// Get other diffables inside the current one
	Children() []Diffable
	// Return actions to drop the current diffable
	Drop() []Action
}
