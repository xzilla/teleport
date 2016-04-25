package action

import (
	"encoding/gob"
	"fmt"
)

type DeleteRow struct {
	SchemaName string
	TableName  string
	PrimaryKey Row
}

// Register type for gob
func init() {
	gob.Register(&DeleteRow{})
}

func (a *DeleteRow) Execute(c *Context) error {
	_, err := c.Tx.Exec(
		fmt.Sprintf(
			`DELETE FROM "%s"."%s" WHERE "%s" = $1;`,
			a.SchemaName,
			a.TableName,
			a.PrimaryKey.Column.Name,
		),
		a.PrimaryKey.Value,
	)

	return err
}

func (a *DeleteRow) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, &a.TableName)
}

func (a *DeleteRow) NeedsSeparatedBatch() bool {
	return false
}
