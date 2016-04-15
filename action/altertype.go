package action

import (
	"encoding/gob"
	"fmt"
)

type AlterType struct {
	SchemaName string
	SourceName string
	TargetName string
}

// Register type for gob
func init() {
	gob.Register(&AlterType{})
}

func (a *AlterType) Execute(c Context) error {
	_, err := c.Tx.Exec(
		fmt.Sprintf("ALTER TYPE %s.\"%s\" RENAME TO %s;", a.SchemaName, a.SourceName, a.TargetName),
	)

	return err
}

func (a *AlterType) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, nil)
}
