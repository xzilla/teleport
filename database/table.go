package database

// Define a database table
type Table struct {
	Name    string
	Columns []Column
}

func NewTable(name string) *Table {
	var t Table
	t.Name = name
	return &t
}
