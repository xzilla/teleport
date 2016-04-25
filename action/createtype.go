package action

import (
	"encoding/gob"
	"fmt"
)

type CreateType struct {
	SchemaName string
	TypeName   string
}

// Register type for gob
func init() {
	gob.Register(&CreateType{})
}

func (a *CreateType) Execute(c *Context) error {
	_, err := c.Tx.Exec(
		fmt.Sprintf(
			"CREATE TYPE \"%s\".\"%s\" AS ENUM ();",
			a.SchemaName,
			a.TypeName,
		),
	)

	return err
}

func (a *CreateType) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, nil)
}

func (a *CreateType) NeedsSeparatedBatch() bool {
	return false
}
