package database

import (
	"encoding/gob"
	"encoding/json"
	"github.com/pagarme/teleport/action"
	// "github.com/pagarme/teleport/batcher/ddldiff"
	"strings"
)

type Dml struct {
	Pre   *map[string]interface{} `json:"pre"`
	Post  *map[string]interface{} `json:"post"`
	Event *Event
	Db    *Database
}

func init() {
	obj := make(map[string]interface{})
	gob.Register(&obj)
}

func NewDml(db *Database, e *Event, data []byte) *Dml {
	var dml Dml
	err := json.Unmarshal(data, &dml)

	if err != nil {
		panic(err)
	}

	dml.Db = db
	dml.Event = e

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

func (d *Dml) GetClass() *Class {
	schemaName, tableName := d.GetSchemaName(), d.GetTableName()

	for _, schema := range d.Db.Schemas {
		if schema.Name == schemaName {
			for _, class := range schema.Classes {
				if class.RelationName == tableName {
					return class
				}
			}
		}
	}

	return nil
}

func (d *Dml) Diff() []action.Action {
	if d.Pre == nil {
		// Insert row
		rows := make([]action.Row, 0)

		// Generate row for each key, value of DML
		for key, value := range *d.Post {
			class := d.GetClass()

			var attribute *Attribute

			for _, att := range class.Attributes {
				if att.Name == key {
					attribute = att
					break
				}
			}

			rows = append(rows, action.Row{
				Value: value,
				Column: action.Column{
					Name: attribute.Name,
					Type: attribute.TypeName,
				},
			})
		}

		return []action.Action{
			&action.InsertRow{
				SchemaName: d.GetSchemaName(),
				TableName:  d.GetTableName(),
				Rows:       rows,
			},
		}
	}

	if d.Post == nil {
		class := d.GetClass()
		pkey := class.GetPrimaryKey()

		return []action.Action{
			&action.DeleteRow{
				SchemaName: d.GetSchemaName(),
				TableName:  d.GetTableName(),
				PrimaryKey: action.Row{
					Value: (*d.Pre)[pkey.Name],
					Column: action.Column{
						Name: pkey.Name,
						Type: pkey.TypeName,
					},
				},
			},
		}
	}

	return []action.Action{}
	// return ddldiff.Diff(
	// 	d.schemaToDiffable(d.PreSchemas),
	// 	d.schemaToDiffable(d.PostSchemas),
	// )
}
