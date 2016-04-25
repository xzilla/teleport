package action

import (
	"encoding/gob"
	"fmt"
)

type AlterIndex struct {
	SchemaName string
	SourceName string
	TargetName string
}

// Register type for gob
func init() {
	gob.Register(&AlterIndex{})
}

func (a *AlterIndex) Execute(c *Context) error {
	_, err := c.Tx.Exec(
		fmt.Sprintf(
			`ALTER INDEX "%s"."%s" RENAME TO "%s";`,
			a.SchemaName,
			a.SourceName,
			a.TargetName,
		),
	)

	return err
}

func (a *AlterIndex) Filter(targetExpression string) bool {
	return true
}

func (a *AlterIndex) NeedsSeparatedBatch() bool {
	return false
}
