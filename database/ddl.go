package database

import (
	"encoding/json"
	"github.com/pagarme/teleport/action"
	"github.com/pagarme/teleport/batcher/ddldiff"
)

type Ddl struct {
	PreSchemas   []*Schema `json:"pre"`
	PostSchemas  []*Schema `json:"post"`
	Db           *Database
	TargetSchema string
}

func NewDdl(db *Database, data []byte, targetSchema string) *Ddl {
	var ddl Ddl
	err := json.Unmarshal(data, &ddl)

	if err != nil {
		panic(err)
	}

	for _, schema := range append(ddl.PreSchemas, ddl.PostSchemas...) {
		schema.fillParentReferences()
		schema.Db = db
	}

	ddl.TargetSchema = targetSchema

	return &ddl
}

func (d *Ddl) schemaToDiffable(schema []*Schema) []ddldiff.Diffable {
	diff := make([]ddldiff.Diffable, 0)

	for i, _ := range schema {
		diff = append(diff, schema[i])
	}

	return diff
}

func (d *Ddl) filterSchemas(schemas []*Schema) []*Schema {
	for _, schema := range schemas {
		if schema.Name == d.TargetSchema {
			return []*Schema{schema}
		}
	}

	return []*Schema{}
}

func (d *Ddl) Diff() []action.Action {
	return ddldiff.Diff(
		d.schemaToDiffable(d.filterSchemas(d.PreSchemas)),
		d.schemaToDiffable(d.filterSchemas(d.PostSchemas)),
		ddldiff.Context{
			Schema: d.TargetSchema,
		},
	)
}
