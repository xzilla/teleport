package ddldiff

type Action struct {
	// CREATE, RENAME or DROP
	Kind string
	// TABLE, SCHEMA, FUNCTION or COLUMN
	Target string
	Object interface{}
}
