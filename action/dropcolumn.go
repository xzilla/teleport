package action

import (
	"encoding/gob"
	"fmt"
)

type DropColumn struct {
	SchemaName string
	TableName  string
	Column     Column
}

// Register type for gob
func init() {
	gob.Register(&DropColumn{})
}

func (a *DropColumn) Execute(c Context) error {
	_, err := c.Tx.Exec(
		fmt.Sprintf("ALTER TABLE \"%s\".\"%s\" DROP COLUMN \"%s\";", a.SchemaName, a.TableName, a.Column.Name),
	)

	return err
}

func (a *DropColumn) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, &a.TableName)
}
