package database

// Define a database table
type Table struct {
	Name    string
	Columns []*Column
	Schema  *Schema
}

func NewTable(name string, schema *Schema) *Table {
	return &Table{
		Name:    name,
		Columns: make([]*Column, 0),
		Schema:  schema,
	}
}

func (t *Table) InstallTriggers() error {
	return nil
}
