package action

import (
	"encoding/gob"
	"fmt"
	"github.com/jmoiron/sqlx"
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

func (a *CreateTable) Execute(tx *sqlx.Tx) error {
	cols := make([]string, 0)

	for _, col := range a.Columns {
		cols = append(cols, fmt.Sprintf("%s %s", col.Name, col.Type))
	}

	_, err := tx.Exec(
		fmt.Sprintf(
			"CREATE TABLE %s.%s (%s);",
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
