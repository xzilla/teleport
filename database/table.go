package database

import (
	"strings"
)

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

// Parses a string in the form "schemaname.table*" and returns all
// the tables under this schema
func (db *Database) tablesForSourceTables(sourceTables string) ([]*Table, error) {
	separator := strings.Split(sourceTables, ".")
	schemaName := separator[0]

	// Fetch schema from database if it's not already loaded
	if db.Schemas[schemaName] == nil {
		if err := db.fetchSchema(schemaName); err != nil {
			return nil, err
		}
	}

	schema := db.Schemas[schemaName]

	prefix := strings.Split(separator[1], "*")[0]

	var tables []*Table

	// Fetch tables with prefix before *
	for _, table := range schema.Tables {
		if strings.HasPrefix(table.Name, prefix) {
			tables = append(tables, table)
		}
	}

	return tables, nil
}
