package database

// Define a table column
type Column struct {
	Name                   string
	DataTypeSchema         string
	DataTypeName           string
	CharacterMaximumLength int
	ConstraintType         string
	table *Table
}

func NewColumn(name string, dataTypeSchema string, dataTypeName string, characterMaximumLength int, constraintType string, table *Table) *Column {
	return &Column{
		Name:                   name,
		DataTypeSchema:         dataTypeSchema,
		DataTypeName:           dataTypeName,
		CharacterMaximumLength: characterMaximumLength,
		ConstraintType:         constraintType,
		table: table,
	}
}
