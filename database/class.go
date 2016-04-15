package database

import (
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/batcher/ddldiff"
	"log"
)

// Define a database table
type Class struct {
	Oid          string       `json:"oid"`
	RelationKind string       `json:"relation_kind"`
	RelationName string       `json:"relation_name"`
	Attributes   []*Attribute `json:"attributes"`
	Schema       *Schema
}

func (c *Class) InstallTriggers() error {
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
		err := currentAction.Execute(action.Context{
			Tx: tx,
			Db: c.Schema.Db.Db,
		})

		if err != nil {
			log.Printf("Error creating triggers on %s: %v", c.RelationName, err)
		}
	}

	return tx.Commit()
}

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

			// Install triggers on table after creation
			post.InstallTriggers()
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
