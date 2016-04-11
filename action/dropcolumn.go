package action

import (
	"encoding/gob"
	"fmt"
	"github.com/jmoiron/sqlx"
)

type DropColumn struct {
	SchemaName string
	TableName  string
	Column Column
}

// Register type for gob
func init() {
	gob.Register(&DropColumn{})
}

func (a *DropColumn) Execute(tx *sqlx.Tx) error {
	_, err := tx.Exec(
		fmt.Sprintf("ALTER TABLE %s.%s DROP COLUMN %s;", a.SchemaName, a.TableName, a.Column.Name),
	)

	return err
}

func (a *DropColumn) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, &a.TableName)
}
