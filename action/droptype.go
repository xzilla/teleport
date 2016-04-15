package action

import (
	"encoding/gob"
	"fmt"
)

type DropType struct {
	SchemaName string
	TypeName   string
}

// Register type for gob
func init() {
	gob.Register(&DropType{})
}

func (a *DropType) Execute(c Context) error {
	_, err := c.Tx.Exec(
		fmt.Sprintf("DROP TYPE \"%s\".\"%s\";", a.SchemaName, a.TypeName),
	)

	return err
}

func (a *DropType) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, &a.TypeName)
}
