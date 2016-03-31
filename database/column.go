package database

// Define a table column
type Column struct {
	Name                   string
	DataTypeSchema         string
	DataTypeName           string
	CharacterMaximumLength int
	ConstraintType         string
}
