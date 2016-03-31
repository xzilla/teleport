package database

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Define a database schema
type Schema struct {
	Name   string
	Tables map[string]*Table
}

// Define the sqlColumn returned inside get_current_schema() query
type sqlColumn struct {
	TableSchema            string `json:"table_schema"`
	TableName              string `json:"table_name"`
	ColumnName             string `json:"column_name"`
	DataType               string `json:"data_type"`
	UdtSchema              string `json:"udt_schema"`
	UdtName                string `json:"udt_name"`
	CharacterMaximumLength int    `json:"character_maximum_length"`
	ContraintType          string `json:"constraint_type"`
}

// Initializes new schema
func NewSchema(name string) *Schema {
	var s Schema
	s.Name = name
	s.Tables = make(map[string]*Table)
	return &s
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

	fmt.Printf("schema: %v\n", schema)
	
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

// Fetches the schema from the database and update Schema
func (db *Database) fetchSchema(schema string) error {
	// Get schema from query
	rows, err := db.runQuery("SELECT get_current_schema();")
	defer rows.Close()

	if err != nil {
		return err
	}

	// Read schema content from sql.Row
	var schemaContent []byte
	rows.Next()
	err = rows.Scan(&schemaContent)

	if err != nil {
		return err
	}

	// Parse JSON array of rows into sqlColumns
	parsedColumns := make([]sqlColumn, 0)
	err = json.Unmarshal(schemaContent, &parsedColumns)

	if err != nil {
		return err
	}

	// Populate db's schema
	for _, sqlCol := range parsedColumns {
		// Create schema if not exists
		if _, ok := db.Schemas[sqlCol.TableSchema]; !ok {
			db.Schemas[sqlCol.TableSchema] = NewSchema(sqlCol.TableSchema)
		}

		schema := db.Schemas[sqlCol.TableSchema]

		// Create table if not exists
		if _, ok := schema.Tables[sqlCol.TableName]; !ok {
			schema.Tables[sqlCol.TableName] = NewTable(sqlCol.TableName)
		}

		table := schema.Tables[sqlCol.TableName]

		// Add column
		table.Columns = append(table.Columns, Column{
			Name:                   sqlCol.ColumnName,
			DataTypeSchema:         sqlCol.UdtSchema,
			DataTypeName:           sqlCol.UdtName,
			CharacterMaximumLength: sqlCol.CharacterMaximumLength,
			ConstraintType:         sqlCol.ContraintType,
		})
	}

	return nil
}
