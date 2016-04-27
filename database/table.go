package database

import (
	"fmt"
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/batcher/ddldiff"
	"log"
)

// Define a database table
type Table struct {
	Oid          string       `json:"oid"`
	RelationKind string       `json:"relation_kind"`
	RelationName string       `json:"relation_name"`
	Attributes   []*Attribute `json:"attributes"`
	Indexes      []*Index     `json:"indexes"`
	Schema       *Schema
}

func (c *Table) InstallTriggers() error {
	// Bail out if there's no schema/db
	if c.Schema == nil || c.Schema.Db == nil {
		return nil
	}

	if c.GetPrimaryKey() == nil {
		return fmt.Errorf("table %s does not have primary key!", c.RelationName)
	}

	log.Printf("Installing triggers for %s.%s...", c.Schema.Name, c.RelationName)

	actions := []action.Action{
		&action.DropTrigger{
			SchemaName:  c.Schema.Name,
			TableName:   c.RelationName,
			TriggerName: "teleport_dml_insert_update_delete",
		},
		&action.CreateTrigger{
			SchemaName:     c.Schema.Name,
			TableName:      c.RelationName,
			TriggerName:    "teleport_dml_insert_update_delete",
			ExecutionOrder: "AFTER",
			Events:         []string{"INSERT", "UPDATE", "DELETE"},
			ProcedureName:  "teleport_dml_event",
		},
	}

	// Start transaction
	tx := c.Schema.Db.NewTransaction()

	for _, currentAction := range actions {
		err := currentAction.Execute(action.NewContext(tx, c.Schema.Db.Db,))

		if err != nil {
			log.Printf("Error creating triggers on %s: %v", c.RelationName, err)
		}
	}

	return tx.Commit()
}

func (c *Table) GetPrimaryKey() *Attribute {
	for _, attr := range c.Attributes {
		if attr.IsPrimaryKey {
			return attr
		}
	}

	return nil
}

// Implements Diffable
func (post *Table) Diff(other ddldiff.Diffable, context ddldiff.Context) []action.Action {
	actions := make([]action.Action, 0)

	// r = Tables
	if post.RelationKind == "r" {
		if other == nil {
			// Install triggers on table after creation
			err := post.InstallTriggers()

			// Warn on errors installing triggers
			if err != nil {
				log.Printf("Error installing triggers on table %s: %v\n", post.RelationName, err)
			}

			primaryKeyAttr := post.GetPrimaryKey()

			actions = append(actions, &action.CreateTable{
				context.Schema,
				post.RelationName,
				action.Column{
					primaryKeyAttr.Name,
					primaryKeyAttr.TypeName,
					primaryKeyAttr.IsNativeType(),
				},
			})
		} else {
			pre := other.(*Table)

			if pre.RelationName != post.RelationName {
				actions = append(actions, &action.AlterTable{
					context.Schema,
					pre.RelationName,
					post.RelationName,
				})
			}
		}
	}

	return actions
}

func (c *Table) Children() []ddldiff.Diffable {
	children := make([]ddldiff.Diffable, 0)
	primaryKey := c.GetPrimaryKey()

	for _, attr := range c.Attributes {
		if primaryKey == nil || attr.Name != primaryKey.Name {
			children = append(children, attr)
		}
	}

	for _, index := range c.Indexes {
		children = append(children, index)
	}

	return children
}

func (c *Table) Drop(context ddldiff.Context) []action.Action {
	if c.RelationKind == "r" {
		return []action.Action{
			&action.DropTable{
				context.Schema,
				c.RelationName,
			},
		}
	} else {
		return []action.Action{}
	}
}

func (c *Table) IsEqual(other ddldiff.Diffable) bool {
	if other == nil {
		return false
	}

	if otherTable, ok := other.(*Table); ok {
		return (c.Oid == otherTable.Oid)
	}

	return false
}
