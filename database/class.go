package database

import (
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/batcher/ddldiff"
)

// Define a database table
type Class struct {
	Oid          string       `json:"oid"`
	RelationKind string       `json:"relation_kind"`
	RelationName string       `json:"relation_name"`
	Attributes   []*Attribute `json:"attributes"`
	Schema       *Schema
}

// func (t *Table) InstallTriggers() error {
// 	return nil
// }

// Parses a string in the form "schemaname.table*" and returns all
// the tables under this schema
// func (db *Database) tablesForSourceTables(sourceTables string) ([]*Class, error) {
// 	return nil, nil
// 	// separator := strings.Split(sourceTables, ".")
// 	// schemaName := separator[0]
// 	//
// 	// // // Fetch schema from database if it's not already loaded
// 	// // if db.Schemas[schemaName] == nil {
// 	// // 	if err := db.fetchSchema(schemaName); err != nil {
// 	// // 		return nil, err
// 	// // 	}
// 	// // }
// 	//
// 	// schema := db.Schemas[schemaName]
// 	//
// 	// prefix := strings.Split(separator[1], "*")[0]
// 	//
// 	// var tables []*Table
// 	//
// 	// // Fetch tables with prefix before *
// 	// for _, table := range schema.Tables {
// 	// 	if strings.HasPrefix(table.Name, prefix) {
// 	// 		tables = append(tables, table)
// 	// 	}
// 	// }
// 	//
// 	// return tables, nil
// }

// Implements Diffable
func (post *Class) Diff(other ddldiff.Diffable) []action.Action {
	actions := make([]action.Action, 0)

	// r = Tables
	if post.RelationKind == "r" {
		if other == nil {
			cols := make([]action.Column, 0)

			for _, attr := range post.Attributes {
				cols = append(cols, action.Column{
					attr.Name,
					attr.TypeName,
				})
			}

			actions = append(actions, &action.CreateTable{
				post.Schema.Name,
				post.RelationName,
				cols,
			})
		} else {
			pre := other.(*Class)

			if pre.RelationName != post.RelationName {
				actions = append(actions, &action.AlterTable{
					post.Schema.Name,
					pre.RelationName,
					post.RelationName,
				})
			}
		}
	}

	return actions
}

func (c *Class) Children() []ddldiff.Diffable {
	children := make([]ddldiff.Diffable, 0)

	for _, attr := range c.Attributes {
		children = append(children, attr)
	}

	return children
}

func (c *Class) Drop() []action.Action {
	if c.RelationKind == "r" {
		return []action.Action{
			&action.DropTable{
				c.Schema.Name,
				c.RelationName,
			},
		}
	} else {
		return []action.Action{}
	}
}

func (c *Class) IsEqual(other ddldiff.Diffable) bool {
	if other == nil {
		return false
	}

	if otherClass, ok := other.(*Class); ok {
		return (c.Oid == otherClass.Oid)
	}
	
	return false
}
