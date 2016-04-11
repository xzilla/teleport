package action

import (
	"encoding/gob"
	"fmt"
	"github.com/jmoiron/sqlx"
)

type AlterTable struct {
	SchemaName string
	SourceName string
	TargetName string
}

// Register type for gob
func init() {
	gob.Register(&AlterTable{})
}

func (a *AlterTable) Execute(tx *sqlx.Tx) error {
	_, err := tx.Exec(
		fmt.Sprintf("ALTER TABLE %s.%s RENAME TO %s;", a.SchemaName, a.SourceName, a.TargetName),
	)

	return err
}

func (a *AlterTable) Filter(targetExpression string) bool {
	return IsInTargetExpression(&targetExpression, &a.SchemaName, &a.SourceName)
}
