package action

import (
	"encoding/gob"
	"fmt"
)

type DropTable struct {
	SchemaName string
	TableName  string
}

// Register type for gob
func init() {
	gob.Register(&DropTable{})
}

func (a *DropTable) Execute(c *Context) error {
	_, err := c.Tx.Exec(
		fmt.Sprintf("DROP TABLE \"%s\".\"%s\";", a.SchemaName, a.TableName),
	)

	return err
}

func (a *DropTable) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, &a.TableName)
}

func (a *DropTable) NeedsSeparatedBatch() bool {
	return false
}
