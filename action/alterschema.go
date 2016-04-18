package action

import (
	"encoding/gob"
	"fmt"
)

type AlterSchema struct {
	SourceName string
	TargetName string
}

// Register type for gob
func init() {
	gob.Register(&AlterSchema{})
}

func (a *AlterSchema) Execute(c Context) error {
	_, err := c.Tx.Exec(
		fmt.Sprintf("ALTER SCHEMA \"%s\" RENAME TO \"%s\";", a.SourceName, a.TargetName),
	)

	return err
}

func (a *AlterSchema) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SourceName, nil)
}

func (a *AlterSchema) NeedsSeparatedBatch() bool {
	return false
}
