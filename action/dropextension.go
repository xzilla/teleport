package action

import (
	"encoding/gob"
	"fmt"
)

type DropExtension struct {
	ExtensionName  string
}

// Register type for gob
func init() {
	gob.Register(&DropExtension{})
}

func (a *DropExtension) Execute(c *Context) error {
	_, err := c.Tx.Exec(
		fmt.Sprintf(
			`DROP EXTENSION IF EXISTS %s WITH SCHEMA %s`,
			a.ExtensionName,
		),
	)

	return err
}

func (a *DropExtension) Filter(targetExpression string) bool {
	return true
}

func (a *DropExtension) NeedsSeparatedBatch() bool {
	return true
}
