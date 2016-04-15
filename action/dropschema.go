package action

import (
	"encoding/gob"
	"fmt"
)

type DropSchema struct {
	SchemaName string
}

// Register type for gob
func init() {
	gob.Register(&DropSchema{})
}

func (a *DropSchema) Execute(c Context) error {
	_, err := c.Tx.Exec(
		fmt.Sprintf("DROP SCHEMA %s;", a.SchemaName),
	)

	return err
}

func (a *DropSchema) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, nil)
}
