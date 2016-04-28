package action

import (
	"encoding/gob"
	"fmt"
)

type CreateAttribute struct {
	SchemaName string
	TypeName   string
	Column     Column
}

// Register type for gob
func init() {
	gob.Register(&CreateAttribute{})
}

func (a *CreateAttribute) Execute(c *Context) error {
	_, err := c.Tx.Exec(
		fmt.Sprintf(
			"ALTER TYPE \"%s\".\"%s\" ADD ATTRIBUTE \"%s\" %s\"%s\";",
			a.SchemaName,
			a.TypeName,
			a.Column.Name,
			a.Column.GetTypeSchemaStr(a.SchemaName),
			a.Column.Type,
		),
	)

	return err
}

func (a *CreateAttribute) Filter(targetExpression string) bool {
	return true
}

func (a *CreateAttribute) NeedsSeparatedBatch() bool {
	return false
}
