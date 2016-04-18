package action

import (
	"encoding/gob"
	"fmt"
)

type AlterTable struct {
	SchemaName string
	SourceName string
	TargetName string
}

// Register type for gob
func init() {
	gob.Register(&AlterTable{})
}

func (a *AlterTable) Execute(c Context) error {
	_, err := c.Tx.Exec(
		fmt.Sprintf("ALTER TABLE \"%s\".\"%s\" RENAME TO \"%s\";", a.SchemaName, a.SourceName, a.TargetName),
	)

	return err
}

func (a *AlterTable) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, &a.SourceName)
}

func (a *AlterTable) NeedsSeparatedBatch() bool {
	return false
}
