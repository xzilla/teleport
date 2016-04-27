package database

import (
	"encoding/gob"
	"encoding/json"
	"github.com/pagarme/teleport/action"
	"sort"
	"strings"
)

type Dml struct {
	Pre          *map[string]interface{} `json:"pre"`
	Post         *map[string]interface{} `json:"post"`
	Event        *Event
	Db           *Database
	TargetSchema string
}

func init() {
	obj := make(map[string]interface{})
	gob.Register(&obj)
}

func NewDml(db *Database, e *Event, data []byte, targetSchema string) *Dml {
	var dml Dml
	err := json.Unmarshal(data, &dml)

	if err != nil {
		panic(err)
	}

	dml.Db = db
	dml.Event = e
	dml.TargetSchema = targetSchema

	return &dml
}

func (d *Dml) GetSchemaName() string {
	separator := strings.Split(d.Event.TriggerTag, ".")
	return separator[0]
}

func (d *Dml) GetTableName() string {
	separator := strings.Split(d.Event.TriggerTag, ".")
	return separator[1]
}

func (d *Dml) GetTable() *Table {
	schemaName, tableName := d.GetSchemaName(), d.GetTableName()

	for _, schema := range d.Db.Schemas {
		if schema.Name == schemaName {
			for _, class := range schema.Tables {
				if class.RelationName == tableName {
					return class
				}
			}
		}
	}

	return nil
}

func (d *Dml) generateRows(obj *map[string]interface{}) []action.Row {
	rows := make([]action.Row, 0)

	// Sort keys to returned rows sorted by name
	var keys []string
	for k := range *obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Generate row for each key, value of DML
	for _, key := range keys {
		value := (*obj)[key]
		class := d.GetTable()

		var column *Column

		for _, att := range class.Columns {
			if att.Name == key {
				column = att
				break
			}
		}

		rows = append(rows, action.Row{
			value,
			action.Column{
				column.Name,
				column.TypeName,
				column.IsNativeType(),
			},
		})
	}

	return rows
}

func (d *Dml) generatePrimaryKeyRow(obj *map[string]interface{}) action.Row {
	class := d.GetTable()
	pkey := class.GetPrimaryKey()

	return action.Row{
		(*obj)[pkey.Name],
		action.Column{
			pkey.Name,
			pkey.TypeName,
			pkey.IsNativeType(),
		},
	}
}

func (d *Dml) Diff() []action.Action {
	// Insert row
	if d.Pre == nil {
		return []action.Action{
			&action.InsertRow{
				d.TargetSchema,
				d.GetTableName(),
				d.GetTable().GetPrimaryKey().Name,
				d.generateRows(d.Post),
			},
		}
	}

	// Delete row
	if d.Post == nil {
		return []action.Action{
			&action.DeleteRow{
				d.TargetSchema,
				d.GetTableName(),
				d.generatePrimaryKeyRow(d.Pre),
			},
		}
	}

	// Update row
	return []action.Action{
		&action.UpdateRow{
			d.TargetSchema,
			d.GetTableName(),
			d.generatePrimaryKeyRow(d.Post),
			d.generateRows(d.Post),
		},
	}
}
