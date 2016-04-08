package database

import (
	"encoding/json"
	"github.com/pagarme/teleport/batcher/ddldiff"
)

// Define a database schema
type Schema struct {
	Oid     string  `json:"oid"`
	Name    string  `json:"schema_name"`
	Classes []Class `json:"classes"`
}

type Schemas []Schema

func ParseSchema(schemaContent string) ([]Schema, error) {
	var schemas Schemas
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
	}

	return nil
}

// Implements Diffable
func (post *Schema) Diff(other ddldiff.Diffable) []ddldiff.Action {
	actions := make([]ddldiff.Action, 0)

	if other == nil {
		actions = append(actions, ddldiff.Action{
			"CREATE",
			"SCHEMA",
			*post,
		})
	} else {
		pre := other.(*Schema)

		if pre.Name != post.Name {
			actions = append(actions, ddldiff.Action{
				"RENAME",
				"SCHEMA",
				*post,
			})
		}
	}

	return actions
}

func (s *Schema) Children() []ddldiff.Diffable {
	children := make([]ddldiff.Diffable, 0)

	for i, _ := range s.Classes {
		children = append(children, &s.Classes[i])
	}

	return children
}

func (s *Schema) Drop() []ddldiff.Action {
	return []ddldiff.Action{
		ddldiff.Action{
			"DROP",
			"SCHEMA",
			*s,
		},
	}
}

func (s *Schema) IsEqual(other ddldiff.Diffable) bool {
	if other == nil {
		return false
	}

	otherSchema := other.(*Schema)
	return (s.Oid == otherSchema.Oid)
}
