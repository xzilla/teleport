package action

import (
	"encoding/gob"
	"fmt"
)

type CreateEnum struct {
	SchemaName string
	TypeName   string
	Name       string
}

// Register type for gob
func init() {
	gob.Register(&CreateEnum{})
}

func (a *CreateEnum) Execute(c Context) error {
	// ALTER TYPE... ADD VALUE cannot run inside a transaction
	_, err := c.Db.Exec(
		fmt.Sprintf(
			"ALTER TYPE \"%s\".\"%s\" ADD VALUE '%s';",
			a.SchemaName,
			a.TypeName,
			a.Name,
		),
	)

	return err
}

func (a *CreateEnum) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, nil)
}

func (a *CreateEnum) NeedsSeparatedBatch() bool {
	return true
}
