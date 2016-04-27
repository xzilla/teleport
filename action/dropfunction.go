package action

import (
	"encoding/gob"
	"fmt"
)

type DropFunction struct {
	SchemaName   string
	FunctionName string
	Arguments    string
}

// Register type for gob
func init() {
	gob.Register(&DropFunction{})
}

func (a *DropFunction) Execute(c *Context) error {
	_, err := c.Tx.Exec(
		fmt.Sprintf(`DROP FUNCTION "%s".%s(%s);`, a.SchemaName, a.FunctionName, a.Arguments),
	)

	return err
}

func (a *DropFunction) Filter(targetExpression string) bool {
	return true
}

func (a *DropFunction) NeedsSeparatedBatch() bool {
	return false
}
