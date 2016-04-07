package database

import (
	"encoding/json"
	"fmt"
)

// Define a database schema
type Schema struct {
	Oid     string  `json:"oid"`
	Name    string  `json:"schema_name"`
	Classes []Class `json:"classes"`
}

func ParseSchema(schemaContent string) ([]Schema, error) {
	schemas := make([]Schema, 0)

	err := json.Unmarshal([]byte(schemaContent), &schemas)

	if err != nil {
		return nil, err
	}

	return schemas, err
}

// Fetches the schema from the database and update Schema
func (db *Database) fetchSchema() error {
	// Get schema from query
	rows, err := db.runQuery("SELECT get_schema();")
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

	// Parse schema
	var schemas []Schema
	schemas, err = ParseSchema(string(schemaContent))

	if err != nil {
		return err
	}

	// Populate db.Schemas
	db.Schemas = make(map[string]Schema)

	for _, schema := range schemas {
		db.Schemas[schema.Name] = schema
		fmt.Printf("schema: %s / val: %v\n", schema.Name, db.Schemas[schema.Name])
	}

	return nil
}
