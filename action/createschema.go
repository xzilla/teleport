package action

import (
	"encoding/gob"
	"fmt"
)

type CreateSchema struct {
	SchemaName string
}

// Register type for gob
func init() {
	gob.Register(&CreateSchema{})
}

func (a *CreateSchema) Execute(c *Context) error {
	_, err := c.Tx.Exec(
		fmt.Sprintf(`
			DO $$
			BEGIN
				IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = '%s') THEN
					CREATE SCHEMA %s;
				END IF;
			END
			$$
		`, a.SchemaName, a.SchemaName),
	)

	return err
}

func (a *CreateSchema) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, nil)
}

func (a *CreateSchema) NeedsSeparatedBatch() bool {
	return false
}
