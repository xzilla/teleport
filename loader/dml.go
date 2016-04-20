package loader

import (
	"fmt"
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/database"
	"strings"
)

func (l *Loader) createDMLEvents() ([]database.Event, error) {
	tx := l.db.NewTransaction()
	events := make([]database.Event, 0)

	for _, schema := range l.db.Schemas {
		for _, class := range schema.Classes {
			if !action.IsInTargetExpression(&l.target.TargetExpression, &schema.Name, &class.RelationName) {
				continue
			}

			// r = regular table
			if class.RelationKind != "r" {
				continue
			}

			event := &database.Event{
				Kind:          "dml",
				Status:        "building",
				TriggerTag:    fmt.Sprintf("%s.%s", schema.Name, class.RelationName),
				TriggerEvent:  "dml_initial_load",
				TransactionId: "0",
				Data:          nil,
			}

			event.InsertQuery(tx)
			events = append(events, *event)
		}
	}
	
	err := tx.Commit()

	if err != nil {
		return []database.Event{}, err
	}

	return events, nil
}

func (l *Loader) resumeDMLEvents(events []database.Event) error {
	for _, event := range events {
		if event.TriggerEvent != "dml_initial_load" {
			continue
		}

		err := l.resumeDMLEvent(&event)

		if err != nil {
			return err
		}
	}

	return nil
}

func (l *Loader) getDMLEventSchemaClass(event *database.Event) (*database.Schema, *database.Class) {
	separator := strings.Split(event.TriggerTag, ".")
	schema := l.db.Schemas[separator[0]]
	var class *database.Class

	for _, c := range schema.Classes {
		if c.RelationName == separator[1] {
			class = c
			break
		}
	}

	return schema, class
}

func (l *Loader) resumeDMLEvent(event *database.Event) error {
	tx := l.db.NewTransaction()

	schema, class := l.getDMLEventSchemaClass(event)

	return tx.Commit()
}
