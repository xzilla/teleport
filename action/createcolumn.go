package action

import (
	"encoding/gob"
	"fmt"
)

type CreateColumn struct {
	SchemaName string
	TableName  string
	Column     Column
}

// Register type for gob
func init() {
	gob.Register(&CreateColumn{})
}

func (a *CreateColumn) Execute(c Context) error {
	_, err := c.Tx.Exec(
		fmt.Sprintf(
			"ALTER TABLE \"%s\".\"%s\" ADD COLUMN \"%s\" %s\"%s\";",
			a.SchemaName,
			a.TableName,
			a.Column.Name,
			a.Column.GetTypeSchemaStr(a.SchemaName),
			a.Column.Type,
		),
	)

	return err
}

func (a *CreateColumn) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, &a.TableName)
}

func (a *CreateColumn) NeedsSeparatedBatch() bool {
	return false
}
