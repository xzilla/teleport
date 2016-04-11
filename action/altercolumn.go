package action

import (
	"encoding/gob"
	"fmt"
	"github.com/jmoiron/sqlx"
)

type AlterColumn struct {
	SchemaName string
	TableName string
	Column Column
	NewColumn Column
}

// Register type for gob
func init() {
	gob.Register(&AlterColumn{})
}

func (a *AlterColumn) Execute(tx *sqlx.Tx) error {
	if a.Column.Name != a.NewColumn.Name {
		_, err := tx.Exec(
			fmt.Sprintf(
				"ALTER TABLE %s.%s RENAME COLUMN %s TO %s;",
				a.SchemaName,
				a.TableName,
				a.Column.Name,
				a.NewColumn.Name,
			),
		)

		if err != nil {
			return err
		}
	}

	if a.Column.Type != a.NewColumn.Type {
		_, err := tx.Exec(
			fmt.Sprintf(
				"ALTER TABLE %s.%s ALTER COLUMN %s TYPE %s;",
				a.SchemaName,
				a.TableName,
				a.NewColumn.Name,
				a.NewColumn.Type,
			),
		)

		if err != nil {
			return err
		}
	}

	return nil
}

func (a *AlterColumn) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, &a.TableName)
}
