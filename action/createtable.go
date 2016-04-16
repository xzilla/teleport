package action

import (
	"encoding/gob"
	"fmt"
	"strings"
)

type CreateTable struct {
	SchemaName string
	TableName  string
	PrimaryKey string
	Columns    []Column
}

// Register type for gob
func init() {
	gob.Register(&CreateTable{})
}

func (a *CreateTable) Execute(c Context) error {
	cols := make([]string, 0)

	for _, col := range a.Columns {
		var primaryKeyStr string

		if a.PrimaryKey == col.Name {
			primaryKeyStr = "PRIMARY KEY"
		}

		cols = append(cols, fmt.Sprintf("\"%s\" \"%s\" %s", col.Name, col.Type, primaryKeyStr))
	}

	_, err := c.Tx.Exec(
		fmt.Sprintf(
			"CREATE TABLE \"%s\".\"%s\" (%s);",
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
