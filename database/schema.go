package database

import (
	"encoding/json"
	"fmt"
)

// Define a database schema
type Schema struct {
	Name   string
	Tables map[string]*Table
}

// Define the
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

// Fetches the tables from
func (db *Database) parseSourceTables(sourceTables string) []*Table {
}

func (db *Database) fetchSchema(schema string) (*Schema, error) {
	// Get schema from query
	rows, err := db.runQuery("SELECT get_current_schema();")
	defer rows.Close()

	if err != nil {
		return nil, err
	}

	// Read schema content from sql.Row
	var schemaContent []byte
	rows.Next()
	err = rows.Scan(&schemaContent)

	if err != nil {
		return nil, err
	}

	// Parse JSON array of rows into sqlColumns
	parsedColumns := make([]sqlColumn, 0)
	err = json.Unmarshal(schemaContent, &parsedColumns)

	if err != nil {
		return nil, err
	}

	// Populate db's schema
	for _, sqlCol := range parsedColumns {
		// Create schema if not exists
		if db.Schemas[sqlCol.TableSchema] == nil {
			db.Schemas[sqlCol.TableSchema] = NewSchema(sqlCol.TableSchema)
		}

		schema := db.Schemas[sqlCol.TableSchema]

		// Create table if not exists
		if schema.Tables[sqlCol.TableName] == nil {
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

	return db.Schema[schema], nil
}
