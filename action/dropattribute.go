package action

import (
	"encoding/gob"
	"fmt"
)

type DropAttribute struct {
	SchemaName string
	TypeName   string
	Column     Column
}

// Register type for gob
func init() {
	gob.Register(&DropAttribute{})
}

func (a *DropAttribute) Execute(c *Context) error {
	_, err := c.Tx.Exec(
		fmt.Sprintf("ALTER TYPE \"%s\".\"%s\" DROP ATTRIBUTE \"%s\";", a.SchemaName, a.TypeName, a.Column.Name),
	)

	return err
}

func (a *DropAttribute) Filter(targetExpression string) bool {
	return true
}

func (a *DropAttribute) NeedsSeparatedBatch() bool {
	return false
}
