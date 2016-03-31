package database

// Define a database table
type Table struct {
	Name    string
	Columns []*Column
	schema  *Schema
}

func NewTable(name string, schema *Schema) *Table {
	return &Table{
		Name: name,
		Columns: make([]*Column, 0),
		schema: schema,
	}
}
