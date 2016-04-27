package action

import (
	"encoding/gob"
	"fmt"
)

type CreateIndex struct {
	SchemaName string
	IndexName  string
	IndexDef   string
}

// Register type for gob
func init() {
	gob.Register(&CreateIndex{})
}

func (a *CreateIndex) Execute(c *Context) error {
	var originalSearchPath string

	err := c.Tx.Get(&originalSearchPath, "SHOW search_path;")

	if err != nil {
		return err
	}

	_, err = c.Tx.Exec(
		fmt.Sprintf("SET search_path = %s;", a.SchemaName),
	)

	if err != nil {
		return err
	}

	_, err = c.Tx.Exec(a.IndexDef)

	if err != nil {
		return err
	}

	_, err = c.Tx.Exec(
		fmt.Sprintf("SET search_path = %s;", originalSearchPath),
	)

	return err
}

func (a *CreateIndex) Filter(targetExpression string) bool {
	return true
}

func (a *CreateIndex) NeedsSeparatedBatch() bool {
	return false
}
