package action

import (
	"encoding/gob"
	"fmt"
	"strings"
)

type CreateTable struct {
	SchemaName string
	TableName  string
	Columns    []Column
}

// Register type for gob
func init() {
	gob.Register(&CreateTable{})
}

func (a *CreateTable) Execute(c Context) error {
	cols := make([]string, 0)

	for _, col := range a.Columns {
		cols = append(cols, fmt.Sprintf("%s %s", col.Name, col.Type))
	}

	_, err := c.Tx.Exec(
		fmt.Sprintf(
			"CREATE TABLE %s.\"%s\" (%s);",
			a.SchemaName,
			a.TableName,
			strings.Join(cols, ","),
		),
	)

	return err
}

func (a *CreateTable) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, &a.TableName)
}
