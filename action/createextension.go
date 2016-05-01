package action

import (
	"encoding/gob"
	"fmt"
)

type CreateExtension struct {
	SchemaName    string
	ExtensionName string
}

// Register type for gob
func init() {
	gob.Register(&CreateExtension{})
}

func (a *CreateExtension) Execute(c *Context) error {
	_, err := c.Tx.Exec(
		fmt.Sprintf(
			`CREATE EXTENSION IF NOT EXISTS %s WITH SCHEMA %s`,
			a.ExtensionName,
			a.SchemaName,
		),
	)

	return err
}

func (a *CreateExtension) Filter(targetExpression string) bool {
	return true
}

func (a *CreateExtension) NeedsSeparatedBatch() bool {
	return true
}
