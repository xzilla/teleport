package action

import (
	"encoding/gob"
	"fmt"
)

type CreateSchema struct {
	SchemaName string
}

// Register type for gob
func init() {
	gob.Register(&CreateSchema{})
}

func (a *CreateSchema) Execute(c *Context) error {
	_, err := c.Tx.Exec(
		fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS \"%s\";", a.SchemaName),
	)

	return err
}

func (a *CreateSchema) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, nil)
}

func (a *CreateSchema) NeedsSeparatedBatch() bool {
	return false
}
