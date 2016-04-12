package database

import (
	"encoding/json"
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/batcher/ddldiff"
)

// Define a database schema
type Schema struct {
	Oid     string   `json:"oid"`
	Name    string   `json:"schema_name"`
	Classes []*Class `json:"classes"`
}

type Schemas []*Schema

func ParseSchema(schemaContent string) (Schemas, error) {
	var schemas Schemas
	err := json.Unmarshal([]byte(schemaContent), &schemas)

	if err != nil {
		return nil, err
	}

	for _, schema := range schemas {
		schema.fillParentReferences()
	}

	return schemas, err
}

// Fill pointers to parent struct in children of schema
func (s *Schema) fillParentReferences() {
	for _, class := range s.Classes {
		class.Schema = s
		for _, attr := range class.Attributes {
			attr.Class = class
		}
	}
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
	var schemas Schemas
	schemas, err = ParseSchema(string(schemaContent))

	if err != nil {
		return err
	}

	for _, schema := range schemas {
		db.Schemas[schema.Name] = schema
	}

	return nil
}

// Implements Diffable
func (post *Schema) Diff(other ddldiff.Diffable) []action.Action {
	actions := make([]action.Action, 0)

	if other == nil {
		actions = append(actions, &action.CreateSchema{
			post.Name,
		})
	} else {
		pre := other.(*Schema)

		if pre.Name != post.Name {
			actions = append(actions, &action.AlterSchema{
				pre.Name,
				post.Name,
			})
		}
	}

	return actions
}

func (s *Schema) Children() []ddldiff.Diffable {
	children := make([]ddldiff.Diffable, 0)

	for i, _ := range s.Classes {
		children = append(children, s.Classes[i])
	}

	return children
}

func (s *Schema) Drop() []action.Action {
	return []action.Action{
		&action.DropSchema{
			s.Name,
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
