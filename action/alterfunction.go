package action

import (
	"encoding/gob"
	"fmt"
)

type AlterFunction struct {
	SchemaName string
	Arguments  string
	SourceName string
	TargetName string
}

// Register type for gob
func init() {
	gob.Register(&AlterFunction{})
}

func (a *AlterFunction) Execute(c *Context) error {
	_, err := c.Tx.Exec(
		fmt.Sprintf(
			`ALTER FUNCTION "%s".%s(%s) RENAME TO "%s";`,
			a.SchemaName,
			a.SourceName,
			a.Arguments,
			a.TargetName,
		),
	)

	return err
}

func (a *AlterFunction) Filter(targetExpression string) bool {
	return true
}

func (a *AlterFunction) NeedsSeparatedBatch() bool {
	return false
}
