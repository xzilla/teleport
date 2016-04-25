package action

import (
	"encoding/gob"
	"fmt"
)

type DropIndex struct {
	SchemaName string
	IndexName  string
}

// Register type for gob
func init() {
	gob.Register(&DropIndex{})
}

func (a *DropIndex) Execute(c *Context) error {
	_, err := c.Tx.Exec(
		fmt.Sprintf(`DROP INDEX "%s"."%s";`, a.SchemaName, a.IndexName),
	)

	return err
}

func (a *DropIndex) Filter(targetExpression string) bool {
	return true
}

func (a *DropIndex) NeedsSeparatedBatch() bool {
	return false
}
