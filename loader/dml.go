package loader

import (
	"fmt"
	"log"
	"github.com/jmoiron/sqlx"
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/database"
	"strings"
)

func (l *Loader) createDMLEvents() ([]*database.Event, error) {
	tx := l.db.NewTransaction()
	events := make([]*database.Event, 0)

	for _, schema := range l.db.Schemas {
		for _, class := range schema.Classes {
			if !action.IsInTargetExpression(&l.target.TargetExpression, &schema.Name, &class.RelationName) {
				continue
			}

			// r = regular table
			if class.RelationKind != "r" {
				continue
			}

			if class.GetPrimaryKey() == nil {
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
			events = append(events, event)
		}
	}

	err := tx.Commit()

	if err != nil {
		return []*database.Event{}, err
	}

	return events, nil
}

func (l *Loader) resumeDMLEvents(events []*database.Event) error {
	for _, event := range events {
		if event.TriggerEvent != "dml_initial_load" {
			continue
		}

		err := l.resumeDMLEvent(event)

		if err != nil {
			return err
		}

		log.Printf("Ended processing event %#v\n", event)
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

func (l *Loader) generateColumnsForAttributes(attributes []*database.Attribute) map[string]action.Column {
	attributeCol := make(map[string]action.Column)

	for _, attr := range attributes {
		attributeCol[attr.Name] = action.Column{
			Name: attr.Name,
			Type: attr.TypeName,
		}
	}

	return attributeCol
}

func (l *Loader) resumeDMLEvent(event *database.Event) error {
	tx := l.db.NewTransaction()

	schema, class := l.getDMLEventSchemaClass(event)
	tableCount, err := l.getTableCount(tx, schema, class)

	if err != nil {
		return err
	}

	colsForAttributes := l.generateColumnsForAttributes(class.Attributes)

	event.Status = "batched"
	event.UpdateQuery(tx)

	// Create a new batch with initial data
	batch := database.NewBatch("fs")
	batch.Source = l.db.Name
	batch.Target = l.targetName
	initialData := ""
	batch.SetData(&initialData)

	batch.InsertQuery(tx)

	// Generate OFFSET/LIMITs to iterate
	for i := 0; i < tableCount; i += l.BatchSize {
		rows, err := l.fetchRows(tx, schema, class, l.BatchSize, i)

		if err != nil {
			return err
		}

		actions := make([]action.Action, 0)

		for _, row := range rows {
			actionRows := make([]action.Row, 0)

			for key, value := range *row {
				actionRows = append(actionRows, action.Row{
					Value:  value,
					Column: colsForAttributes[key],
				})
			}

			act := &action.InsertRow{
				SchemaName: schema.Name,
				TableName:  class.RelationName,
				Rows:       actionRows,
			}

			actions = append(actions, act)
		}

		err = batch.AppendActions(actions)

		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (l *Loader) getTableCount(tx *sqlx.Tx, schema *database.Schema, table *database.Class) (int, error) {
	var count int

	err := tx.Get(&count,
		fmt.Sprintf(
			`SELECT count(*) FROM "%s"."%s";`,
			schema.Name,
			table.RelationName,
		),
	)

	return count, err
}

func (l *Loader) fetchRows(tx *sqlx.Tx, schema *database.Schema, table *database.Class, limit, offset int) ([]*map[string]interface{}, error) {
	result := make([]*map[string]interface{}, 0)

	rows, err := tx.Queryx(
		fmt.Sprintf(
			`SELECT * FROM "%s"."%s" ORDER BY "%s" LIMIT %d OFFSET %d;`,
			schema.Name,
			table.RelationName,
			table.GetPrimaryKey().Name,
			limit,
			offset,
		),
	)

	if err != nil {
		return []*map[string]interface{}{}, err
	}

	for rows.Next() {
		results := make(map[string]interface{})
		err = rows.MapScan(results)

		if err != nil {
			return []*map[string]interface{}{}, err
		}

		result = append(result, &results)
	}

	return result, nil
}
