package action

import (
	"encoding/gob"
	"fmt"
	"github.com/jmoiron/sqlx"
)

type CreateColumn struct {
	SchemaName string
	TableName  string
	Column     Column
}

// Register type for gob
func init() {
	gob.Register(&CreateColumn{})
}

func (a *CreateColumn) Execute(tx *sqlx.Tx) error {
	_, err := tx.Exec(
		fmt.Sprintf(
			"ALTER TABLE %s.%s ADD COLUMN %s %s;",
			a.SchemaName,
			a.TableName,
			a.Column.Name,
			a.Column.Type,
		),
	)

	return err
}

func (a *CreateColumn) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, &a.TableName)
}
