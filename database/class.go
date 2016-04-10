package database

import (
	// "github.com/pagarme/teleport/batcher/ddldiff"
)

// Define a database table
type Class struct {
	Oid          string      `json:"oid"`
	RelationKind string      `json:"relation_kind"`
	RelationName string      `json:"relation_name"`
	Attributes   []Attribute `json:"attributes"`
}

// func (t *Table) InstallTriggers() error {
// 	return nil
// }

// Parses a string in the form "schemaname.table*" and returns all
// the tables under this schema
func (db *Database) tablesForSourceTables(sourceTables string) ([]*Class, error) {
	return nil, nil
	// separator := strings.Split(sourceTables, ".")
	// schemaName := separator[0]
	//
	// // // Fetch schema from database if it's not already loaded
	// // if db.Schemas[schemaName] == nil {
	// // 	if err := db.fetchSchema(schemaName); err != nil {
	// // 		return nil, err
	// // 	}
	// // }
	//
	// schema := db.Schemas[schemaName]
	//
	// prefix := strings.Split(separator[1], "*")[0]
	//
	// var tables []*Table
	//
	// // Fetch tables with prefix before *
	// for _, table := range schema.Tables {
	// 	if strings.HasPrefix(table.Name, prefix) {
	// 		tables = append(tables, table)
	// 	}
	// }
	//
	// return tables, nil
}

// // Implements Diffable
// func (post *Class) Diff(other ddldiff.Diffable) []ddldiff.Action {
// 	actions := make([]ddldiff.Action, 0)
//
// 	if other == nil {
// 		actions = append(actions, ddldiff.Action{
// 			"CREATE",
// 			"TABLE",
// 			*post,
// 		})
// 	} else {
// 		pre := other.(*Class)
//
// 		if pre.RelationName != post.RelationName {
// 			actions = append(actions, ddldiff.Action{
// 				"RENAME",
// 				"TABLE",
// 				*post,
// 			})
// 		}
// 	}
//
// 	return actions
// }
//
// func (c *Class) Children() []ddldiff.Diffable {
// 	children := make([]ddldiff.Diffable, 0)
//
// 	// for _, attr := range c.Attributes {
// 	// 	children = append(children, attr)
// 	// }
//
// 	return children
// }
//
// func (c *Class) Drop() []ddldiff.Action {
// 	return []ddldiff.Action{
// 		ddldiff.Action{
// 			"DROP",
// 			"TABLE",
// 			*c,
// 		},
// 	}
// }
//
// func (c *Class) IsEqual(other ddldiff.Diffable) bool {
// 	if other == nil {
// 		return false
// 	}
//
// 	otherClass := other.(*Class)
// 	return (c.Oid == otherClass.Oid)
// }
