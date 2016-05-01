package action

import (
	"encoding/gob"
	"fmt"
)

type AlterAttribute struct {
	SchemaName string
	TypeName   string
	Column     Column
	NewColumn  Column
}

// Register type for gob
func init() {
	gob.Register(&AlterAttribute{})
}

func (a *AlterAttribute) Execute(c *Context) error {
	if a.Column.Name != a.NewColumn.Name {
		_, err := c.Tx.Exec(
			fmt.Sprintf(
				"ALTER TYPE \"%s\".\"%s\" RENAME ATTRIBUTE \"%s\" TO \"%s\";",
				a.SchemaName,
				a.TypeName,
				a.Column.Name,
				a.NewColumn.Name,
			),
		)

		if err != nil {
			return err
		}
	}

	if a.Column.Type != a.NewColumn.Type {
		_, err := c.Tx.Exec(
			fmt.Sprintf(
				"ALTER TYPE \"%s\".\"%s\" ALTER ATTRIBUTE \"%s\" TYPE %s\"%s\";",
				a.SchemaName,
				a.TypeName,
				a.NewColumn.Name,
				a.NewColumn.GetTypeSchemaStr(a.SchemaName),
				a.NewColumn.Type,
			),
		)

		if err != nil {
			return err
		}
	}

	return nil
}

func (a *AlterAttribute) Filter(targetExpression string) bool {
	return true
}

func (a *AlterAttribute) NeedsSeparatedBatch() bool {
	return false
}
