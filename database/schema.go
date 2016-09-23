package database

import (
	"encoding/json"

	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/batcher/ddldiff"
)

// Define a database schema
type Schema struct {
	Oid        string       `json:"oid"`
	Name       string       `json:"schema_name"`
	Tables     []*Table     `json:"classes"`
	Types      []*Type      `json:"types"`
	Functions  []*Function  `json:"functions"`
	Extensions []*Extension `json:"extensions"`
	Db         *Database
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
	for _, class := range s.Tables {
		class.Schema = s
		for _, attr := range class.Columns {
			attr.Table = class
		}
	}

	for _, typ := range s.Types {
		typ.Schema = s
		for _, enum := range typ.Enums {
			enum.Type = typ
		}
		for _, attr := range typ.Attributes {
			attr.Type = typ
		}
	}
}

// Fetches the schema from the database and update Schema
func (db *Database) RefreshSchema() error {
	// Get schema from query
	rows, err := db.runQuery(nil, "SELECT teleport.get_schema();")

	if err != nil {
		return err
	}

	defer rows.Close()

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
		schema.Db = db
	}

	return nil
}

// Implements Diffable
func (post *Schema) Diff(other ddldiff.Diffable, context ddldiff.Context) []action.Action {
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

	for i, _ := range s.Extensions {
		children = append(children, s.Extensions[i])
	}

	// Add enums first
	for i, typ := range s.Types {
		if typ.Type == "e" {
			children = append(children, s.Types[i])
		}
	}

	// ... then composite types...
	for i, typ := range s.Types {
		if typ.Type == "c" {
			children = append(children, s.Types[i])
		}
	}

	for i, _ := range s.Tables {
		children = append(children, s.Tables[i])
	}

	for i, _ := range s.Functions {
		children = append(children, s.Functions[i])
	}

	return children
}

func (s *Schema) Drop(context ddldiff.Context) []action.Action {
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
