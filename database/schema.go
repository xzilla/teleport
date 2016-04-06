package database

import (
	"encoding/json"
)

// Define a database schema
type Schema struct {
	Name     string
	Tables   map[string]*Table
	Database *Database
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
func NewSchema(db *Database, schemaData string) *Schema {
	// Parse JSON array of rows into sqlColumns
	parsedColumns := make([]sqlColumn, 0)
	err := json.Unmarshal([]byte(schemaData), &parsedColumns)

	if err != nil {
		panic(err)
	}

	if len(parsedColumns) == 0 {
		return nil
	}

	schema := &Schema{
		Name: parsedColumns[0].TableSchema,
		Tables:   make(map[string]*Table),
		Database: db,
	}

	// Populate db's schema
	for _, sqlCol := range parsedColumns {
		// Create table if not exists
		if _, ok := schema.Tables[sqlCol.TableName]; !ok {
			schema.Tables[sqlCol.TableName] = NewTable(sqlCol.TableName, schema)
		}

		table := schema.Tables[sqlCol.TableName]

		// Add column
		table.Columns = append(table.Columns, NewColumn(
			sqlCol.ColumnName,
			sqlCol.UdtSchema,
			sqlCol.UdtName,
			sqlCol.CharacterMaximumLength,
			sqlCol.ContraintType,
			table,
		))
	}

	return schema
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

	db.Schemas[schema] = NewSchema(db, string(schemaContent))

	return nil
}
